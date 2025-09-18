# Copyright (c) Microsoft Corporation. All rights reserved.
from .engineapi.typedefinitions import (AnonymousBlockData, ExportScriptFormat,
                                        ExportScriptMessageArguments, ActivityReference,
                                        CreateAnonymousReferenceMessageArguments, PropertyOverride,
                                        ExecutorType)
from .engineapi.api import EngineAPI, get_engine_api
from importlib import import_module
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
import atexit
import base64
import json
import os
import shutil


class DataPrepImportError(Exception):
    pass

class DataPrepSparkVersionError(Exception):
    pass

def _register_directory_for_cleanup(directory_path):
    def remove_directory(directory):
        shutil.rmtree(directory, ignore_errors=True)
    atexit.register(remove_directory, directory_path)


def _get_temp_file_path():
    file = NamedTemporaryFile()
    name = file.name
    file.close()
    return name


def _add_spark_files(directory: str, main_file: str):
    import pyspark
    from pyspark.sql.utils import IllegalArgumentException
    sc = pyspark.SparkContext.getOrCreate()
    _add_lariat_spark_jar(sc)
    for item in os.listdir(directory):
        filename = os.path.join(directory, item)
        try:
            if filename[-3:].lower() == ".py" or filename[-4:].lower() == ".zip":
                sc.addPyFile(filename)
            else:
                sc.addFile(filename, recursive=True)
        except IllegalArgumentException:
            # IllegalArgumentException will be thrown if same file name but different path are being added to the
            # spark context. The reason we are catching is because there are files that are common but also files
            # that are unique to each execution. e.g. <guid>.scala - file with expressions
            pass
    # we are assuming the main file will have a unique file name each time
    sc.addPyFile(main_file)


# noinspection PyUnresolvedReferences
def _add_lariat_spark_jar(sc: 'pyspark.SparkContext'):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # noinspection PyProtectedMember
    scala_version = sc._jvm.scala.util.Properties.versionNumberString()
    first_dot = scala_version.find('.')
    second_dot = first_dot + scala_version[first_dot + 1:].find('.')
    short_scala_version = scala_version[:second_dot + 1]

    if short_scala_version != "2.12" and short_scala_version != "2.11":
        error_message = "Unable to load data as Spark with Scala " + short_scala_version + " is not supported. Only Spark with Scala 2.11 or 2.12 is supported."
        print(error_message)
        raise DataPrepSparkVersionError(error_message)

    lariat_spark_path = os.path.join(current_dir, 'engineapi', 'lariatSpark', 'dprep-' + short_scala_version + '.jar')

    loader = _get_mutable_class_loader(sc)
    if loader is not None:
        # noinspection PyBroadException
        try:
            # noinspection PyProtectedMember
            sc._jsc.addJar(lariat_spark_path)
            # noinspection PyProtectedMember
            file = sc._jvm.java.io.File(lariat_spark_path).toURI().toURL()
            loader.addURL(file)
        except Exception as e:
            print("Exception while adding url : " + e)
            pass


# noinspection PyUnresolvedReferences
def _get_mutable_class_loader(sc: 'pyspark.SparkContext'):
    databricks_loader_class = 'com.databricks.backend.daemon.driver.DriverLocal$DriverLocalClassLoader'

    # noinspection PyBroadException
    try:
        # noinspection PyProtectedMember
        # Loading this at runtime runs into some difficulties. Spark exposes an addJar method on the Java SparkContext
        # which will ship the jar to all worker nodes and add it to their classpaths. We can call into this method from
        # Python. However, this does not add the jar to the driver's classpath. Achieving that requires some unsightly
        # hackery. We can grab the ClassLoader for the driver by using the getContextOrSparkClassLoader method in Spark
        # utils. Spark usually uses a MutableURLClassLoader which exposes an addURL method. If we add the path to our jar
        # this way, then the driver will be able to load the classes contained in it. This works great for Spark running
        # in client and cluster mode, but does not work on Databricks. This is because Databricks uses their own
        # ClassLoader hierarchy, different than the one Spark uses by default. Their core class loader still exposes an
        # addURL method we can use, but there is a class loader on the way there (the DriverClassLoader) which exposes an
        # addURL method we do not want. We skip that one by excluding class loaders of that specific class.

        loader = sc._jvm.org.apache.spark.util.Utils.getContextOrSparkClassLoader()
        while loader is not None:
            if loader.getClass().getName() == databricks_loader_class:
                loader = loader.getParent().getParent()
                continue
            elif hasattr(loader, 'addURL'):
                break
            loader = loader.getParent()

        return loader
    except Exception as e:
        print("Exception while getting loader : " + e)
        return None


def _import_loader(loader_dir: str):
    # Python seems to be tolerant to deleting the modules immediately after import
    # but deferring their deletion until process exit is safer.
    _register_directory_for_cleanup(loader_dir)

    # The generated module is always loader.py but we need a unique one to avoid module name clashes.
    loader_module_name = "loader" + uuid4().hex
    loader_module_file = os.path.join(loader_dir, loader_module_name + ".py")
    os.rename(
        os.path.join(loader_dir, "loader.py"),
        loader_module_file)

    _add_spark_files(loader_dir, loader_module_file)
    return import_module(loader_module_name)


class SparkExecutor:
    def __init__(self, engine_api: EngineAPI):
        self._engine_api = engine_api

    # noinspection PyUnresolvedReferences
    def get_dataframe(self,
                      steps: List[AnonymousBlockData],
                      use_sampling: bool = True,
                      overrides: Optional[List[PropertyOverride]] = None,
                      use_first_record_schema: bool = False) -> 'pyspark.sql.DataFrame':
        return self._execute(steps,
                             ExportScriptFormat.PYSPARKDATAFRAMELOADER,
                             use_sampling,
                             overrides,
                             use_first_record_schema)

    def execute(self,
                steps: List[AnonymousBlockData],
                use_sampling: bool = True,
                overrides: Optional[List[PropertyOverride]] = None,
                use_first_record_schema: bool = False) -> None:
        return self._execute(steps,
                             ExportScriptFormat.PYSPARKRUNFUNCTION,
                             use_sampling,
                             overrides,
                             use_first_record_schema)

    def _execute(self,
                 blocks: List[AnonymousBlockData],
                 export_format: ExportScriptFormat,
                 use_sampling: bool = True,
                 overrides: Optional[List[PropertyOverride]] = None,
                 use_first_record_schema: bool = False) -> Any:
        activity = self._engine_api.create_anonymous_reference(
            CreateAnonymousReferenceMessageArguments(blocks))
        module, secrets = self._export_to_module(activity, export_format, use_sampling, overrides)
        try:
            if export_format == ExportScriptFormat.PYSPARKDATAFRAMELOADER:
                return module.LoadData(secrets=secrets, schemaFromFirstRecord=use_first_record_schema)
            else:
                module.run_dataflow(secrets=secrets)
        except Exception as e:
            lariat_version =\
                e.args[1].get('lariat_version') if e.args and len(e.args) >= 2 and isinstance(e.args[1], dict) else None
            if lariat_version:
                raise DataPrepImportError('Unable to load the data preparation scale-out library for version '
                                          + lariat_version + '.')
            raise e

    def _export_to_module(self,
                          activity: ActivityReference,
                          export_format: ExportScriptFormat,
                          use_sampling: bool,
                          overrides: Optional[List[PropertyOverride]] = None) -> Tuple[Any, Dict[str, str]]:
        output, gathered_secrets = self._export_script(activity=activity,
                                                       export_format=export_format,
                                                       use_sampling=use_sampling,
                                                       overrides=overrides)
        secrets = {secret.key: secret.value for secret in gathered_secrets}
        module = _import_loader(output)
        return module, secrets

    def _export_script(self,
                       activity: ActivityReference,
                       export_format: ExportScriptFormat,
                       use_sampling: bool,
                       overrides: Optional[List[PropertyOverride]] = None):
        output = _get_temp_file_path()
        args = ExportScriptMessageArguments(activity_reference=activity,
                                            format=export_format,
                                            overrides=overrides,
                                            path=output,
                                            use_sampling=use_sampling)
        gathered_secrets = self._engine_api.export_script(args)
        return output, gathered_secrets


def _ensure_interactive_spark(requests_channel):
    def handle_spark_collect(request, writer, socket):
        try:
            module = _import_loader(request['path'])
            lariat_dataset = module.get_lariat_dataset(request['secrets'])
            data_bytes = lariat_dataset.toBytes()
            data_str = base64.b64encode(data_bytes)
            writer.write(json.dumps({'result': 'success', 'data': data_str.decode('utf-8')}))
        except Exception as e:
            writer.write(json.dumps({'result': 'error', 'error': str(e)}))

    def handle_spark_run(request, writer, socket):
        try:
            module = _import_loader(request['path'])
            module.run_dataflow(request['secrets'])
            writer.write(json.dumps({'result': 'success'}))
        except Exception as e:
            writer.write(json.dumps({'result': 'error', 'error': str(e)}))

    requests_channel.register_handler('spark_collect', handle_spark_collect)
    requests_channel.register_handler('spark_run', handle_spark_run)


def set_execution_mode(mode: str):
    """
    Sets the execution mode.

    .. remarks::

        Two modes of execution are supported: 'python' and 'spark'. In 'python' mode execution will be run in the
        current Python process, making use of multiple threads. In 'spark' mode, execution will call into Spark,
        allowing for distributed execution.

    :param mode: The mode of execution. Possible values are 'python' and 'spark'.
    """
    if mode == 'python':
        get_engine_api().set_executor(ExecutorType.CLEX)
    elif mode == 'spark':
        get_engine_api().set_executor(ExecutorType.INTERACTIVESPARK)
    else:
        raise ValueError('Invalid execution mode.')
