#!/usr/bin/env python3
import argparse
import azureml.dataprep as dprep
import shutil
import os


def _get_dataflow(package_path: str, dataflow_name: str = None) -> dprep.Dataflow:
    package = dprep.Package.open(package_path)
    if dataflow_name is None:
        if len(package.dataflows) == 1:
            return package.dataflows[0]
        else:
            raise ValueError('The package specified has multiple Dataflows.'
                             ' Use the --dataflow option to select which one to use.')
    else:
        return package[dataflow_name]


def _execute_package(df: dprep.Dataflow, spark: bool = False):
    print('Starting Dataflow execution...')
    if spark:
        df.run_spark()
    else:
        df.run_local()
    print('Dataflow execution complete.')


high_priority_stats = ['Name', 'Type', 'Count', 'Missing', 'Errors',
                       'Min', 'Max', 'Mean', 'Std.Dev.', 'Var.',
                       '1%', '25%', '50%', '75%', '99%']

stat_names = {
    'Missing Count': 'Missing',
    'Error Count': 'Errors',
    'Standard Deviation': 'Std.Dev.',
    'Variance': 'Var.',
    '1% Quantile' : '1%',
    '25% Quantile': '25%',
    '50% Quantile': '50%',
    '75% Quantile': '75%',
    '99% Quantile': '99%'
}


def _str(value) -> str:
    if isinstance(value, float) and int(value) == value:
        return str(int(value))
    else:
        return str(value)


def _pretty_print_profile(profile: dprep.DataProfile):
    columns = profile.columns
    available_stats = ['Name'] + [stat_names.get(s) or s for s in dprep.ColumnProfile._STAT_COLUMNS]
    high_priority_indices = [available_stats.index(stat) for stat in high_priority_stats]
    stat_indices = high_priority_indices + [i for i in range(len(available_stats)) if i not in high_priority_indices]
    columns_stats = [available_stats] + [[c] + p.get_stats() for c, p in columns.items()]
    lengths = [sorted([_get_length(c, stat_index) for c in columns_stats], reverse=True)[0] for stat_index in stat_indices]
    console_width = shutil.get_terminal_size()[0]
    total_width = 0
    last_index = 0
    for i in range(len(lengths)):
        total_width = total_width + lengths[i] + 1
        if total_width > console_width:
            break
        last_index = i

    profile_str = ''
    for column in columns_stats:
        for j in range(len(stat_indices[:last_index])):
            length = lengths[j]
            stat = column[stat_indices[j]]
            str_valule = _str(stat).ljust(length)
            profile_str = profile_str + str_valule + ' '

        profile_str = profile_str + '\n'

    print(profile_str)


def _get_length(c, stat_index):
    return len(str(c[stat_index])) if c is not None else 0


def _profile_package(df: dprep.Dataflow):
    profile = df.get_profile()
    _pretty_print_profile(profile)


def main():
    parser = argparse.ArgumentParser(description="Azure Machine Learning DataPrep")
    parser.add_argument('action', action='store', choices=['execute', 'profile'])
    parser.add_argument('target', action='store', metavar='<package.dprep>|<myFile.csv>')
    parser.add_argument('--dataflow', '--df', action='store', metavar='<DataflowName>')
    parser.add_argument('--spark', '-s', action='store_true')
    parser.add_argument('--data', '-d', action='store_true')
    parser.add_argument('--package', '-p', action='store_true')
    args = parser.parse_args()

    if not args.data and not args.package:
        target = args.target
        file, ext = os.path.splitext(target)
        if ext == '.dprep':
            args.package = True
        else:
            args.data = True

    if args.data:
        df = dprep.auto_read_file(args.target)
    else:
        df = _get_dataflow(args.target, args.dataflow)

    action = args.action
    if action == 'execute':
        _execute_package(df, args.spark)
    elif action == 'profile':
        _profile_package(df)
    else:
        raise ValueError('Invalid action')


if __name__ == '__main__':
    main()
