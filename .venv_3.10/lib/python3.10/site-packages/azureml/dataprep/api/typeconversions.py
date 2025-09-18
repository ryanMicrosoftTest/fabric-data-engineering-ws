# Copyright (c) Microsoft Corporation. All rights reserved.
from .engineapi.typedefinitions import FieldType, DecimalMark
from typing import List, Dict


class TypeConverter:
    """
    Basic type converter.

    :param data_type: Target type for the conversion.
    """
    def __init__(self, data_type: FieldType):
        self.data_type = data_type


class FloatConverter(TypeConverter):
    """
    Converter to Float.

    :param decimal_mark: Dot "." or  comma "," for different regions' standard symbol for the decimal place.
        Uses a dot decimal marker by default. For example, the number 1234.56 should use "." as the `decimal mark`
        and the number 1234,56 should use "," as the decimal mark.
    """
    def __init__(self, decimal_mark: "."):
        if decimal_mark == ",":
            decimal_mark = DecimalMark.COMMA
        elif decimal_mark == ".":
            decimal_mark = DecimalMark.DOT
        else:
            raise ValueError('Unable to use "{0}" as decimal mark for decimal conversion. Please use a dot "." or comma ",".'.format(decimal_mark))
        super().__init__(FieldType.DECIMAL)
        self.decimal_mark = decimal_mark


class DateTimeConverter(TypeConverter):
    """
    Converter to DateTime.

    :param formats: List of date formats to try during conversion. Like: `%d-%m-%Y` or `%Y-%m-%dT%H:%M:%S.%f`.
    """
    def __init__(self, formats: List[str]):
        super().__init__(FieldType.DATE)
        self.formats = formats


class StreamInfoConverter(TypeConverter):
    """
    Converter to StreamInfo.

    :param workspace: Workspace to query datastore information from.
    """
    def __init__(self, workspace, escaped=True):
        super().__init__(FieldType.STREAM)
        self.workspace = workspace
        self.escaped = escaped


class NoopConverter:
    """
    No-op converter, meaning that data is already on the typed form and does not require conversion.
    """
    pass


class CandidateConverter:
    """
    Result of type inference returned by DataPrep to suggest a potential type conversion.

    :param data_type: Target type for the conversion.
    """
    def __init__(self, data_type: FieldType):
        self.data_type = data_type

    @property
    def is_valid(self):
        """
        If the converter is valid.
        """
        return True

    def __repr__(self):
        return str(self.data_type)


class CandidateDateTimeConverter(CandidateConverter):
    """
    Specialized result of type inference used by DataPrep to suggest DateTime conversion.

    .. remarks::

        It can be in valid or invalid state.
        Valid state means that, based on the scanned data, a list of unambiguous formats was detected and
            DateTimeConverter could be created from this candidate.
        Invalid state means that either sampled values seen during inference were inconclusive (like with `1/1/2018` it is unclear if day is before month) or conflicting.

    :var formats: Unambiguous date formats detected during type inference.
    :var ambiguous_formats: Ambiguous date formats detected during type inference.
    """
    def __init__(self, formats: List[str], ambiguous_formats: List[List[str]]):
        super().__init__(FieldType.DATE)
        self.formats = formats
        self.ambiguous_formats = ambiguous_formats

    @CandidateConverter.is_valid.getter
    def is_valid(self):
        """
        If the converter is valid and has no ambiguous datetime format.
        """
        return not self.ambiguous_formats

    @property
    def can_convert(self):
        """
        If there is any datetime format to be used for convertion.
        """
        return len(self.formats) > 0

    def resolve_ambiguity(self, day_first: bool):
        """
        Resolves date format ambiguity by keeping only one kind of formats.

        :param day_first: Controls which format to preserve. `True` will keep only formats where day comes before month.
        """
        picked_formats = [self._pick_format(possible_formats, day_first) for possible_formats in self.ambiguous_formats]
        self.formats += picked_formats
        self.ambiguous_formats = []

    def _get_format_variants(self) -> List[List[str]]:
        if len(self.ambiguous_formats) == 0:
            return [self.formats]
        day_first_formats = [self._pick_format(possible_formats, True) for possible_formats in self.ambiguous_formats]
        month_first_formats = [self._pick_format(possible_formats, False) for possible_formats in self.ambiguous_formats]
        return [day_first_formats + self.formats, month_first_formats + self.formats]

    @staticmethod
    def _pick_format(formats: List[str], day_first: bool) -> str:
        for candidate_format in formats:
            day_index = candidate_format.index('%d')
            month_index = candidate_format.index('%m')
            if day_first and day_index < month_index:
                return candidate_format
            elif not day_first and month_index < day_index:
                return candidate_format

        raise ValueError('Unable to resolve ambiguity.')

    def __repr__(self):
        variants = self._get_format_variants()
        result_str = """
    """ if len(variants) > 1 else ''

        return result_str + """,
    """.join(["""(FieldType.DATE, {0!s})""".format(variant) for variant in variants])


class InferenceInfo:
    """
    Result of running type inference on a specific column.

    :var converters: List of candidate converters to choose from.
    :vartype converters: builtin.list[azureml.dataprep.CandidateConverter]
    """
    def __init__(self, converters: List[CandidateConverter]):
        self.candidate_converters = converters

    def __repr__(self):
        return repr(self.candidate_converters)


def converter_from_candidate(candidate: CandidateConverter) -> TypeConverter:
    if isinstance(candidate, CandidateDateTimeConverter):
        if not candidate.is_valid:
            raise ValueError('Invalid candidate cannot be turned into a converter.')

        if not candidate.can_convert:
            return NoopConverter()

        return DateTimeConverter(candidate.formats)
    else:
        return TypeConverter(candidate.data_type)


def get_converters_from_candidates(column_candidates: Dict[str, List[CandidateConverter]]) -> Dict[str, TypeConverter]:
    def _pick_candidate(column: str, candidates: List[CandidateConverter]):
        if not isinstance(candidates, List):
            return candidates
        try:
            return next(converter_from_candidate(candidate) for candidate in candidates if candidate.is_valid)
        except StopIteration:
            raise ValueError('No valid candidates for column "{0}".'.format(column))

    converters = {col: _pick_candidate(col, candidates) for col, candidates in column_candidates.items()}

    # if we already had date columns, inference returns DateConverter with no formats. We need to filter those out
    valid_converters = {col: converter for (col, converter) in converters.items() if not isinstance(converter, NoopConverter)}
    return valid_converters
