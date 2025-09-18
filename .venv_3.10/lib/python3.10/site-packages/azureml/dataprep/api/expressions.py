# Copyright (c) Microsoft Corporation. All rights reserved.
""" Contains all supported Azure Machine Learning data preparation expressions, and the methods to create them.
"""

from .value import to_dprep_value, to_dprep_value_and_type, field_type_to_string
from typing import Any, Dict, List, TypeVar, Optional
from .engineapi.typedefinitions import FieldType


def _ensure_expression(value):
    return ValueExpression(value) if not isinstance(value, Expression) else value


def _assert_expression(value):
    if not isinstance(value, Expression):
        raise TypeError('An expression was expected, but found a value instead. You can use the expression builders '
                        'in the expressions module to create one.')


def _expression_input(fn):
    def ensured_fn(*args):
        args = [args[0]] + [_ensure_expression(arg) for arg in args[1:]]
        return fn(*args)

    return ensured_fn


class Expression:
    """
    Represents a data preparation expression.
    """

    def __init__(self, underlying_data: Any, return_type: FieldType = None):
        self.return_type = return_type
        self.underlying_data = underlying_data

    @classmethod
    def from_pod(cls, pod):
        return cls(pod)

    def to_pod(self):
        return self.underlying_data

    @staticmethod
    def _validate_type(expression: 'Expression', expected_type: FieldType):
        if expression is not None and expression.return_type is not None and expression.return_type != expected_type:
            raise TypeError('Unexpected type received. Expected: ' + field_type_to_string(expected_type))

    @_expression_input
    def __eq__(self, other: Any) -> 'Expression':
        return InvokeExpression(IdentifierExpression('Value_Equals'), [self, other], return_type=FieldType.BOOLEAN)

    @_expression_input
    def __ne__(self, other: Any) -> 'Expression':
        return NotExpression(InvokeExpression(IdentifierExpression('Value_Equals'), [self, other]))

    def __invert__(self) -> 'Expression':
        return NotExpression(self)

    def __and__(self, other: 'Expression') -> 'Expression':
        return f_and(self, other)

    def __or__(self, other: 'Expression') -> 'Expression':
        return f_or(self, other)

    def __bool__(self):
        raise RuntimeError('Data prep expressions cannot be used in conditional expressions.')


class FunctionSupportingExpression(Expression):
    @_expression_input
    def __lt__(self, other: Any) -> 'Expression':
        return InvokeExpression(IdentifierExpression('Value_LT'), [self, other], return_type=FieldType.BOOLEAN)

    @_expression_input
    def __le__(self, other: Any) -> 'Expression':
        return InvokeExpression(IdentifierExpression('Value_LE'), [self, other], return_type=FieldType.BOOLEAN)

    @_expression_input
    def __gt__(self, other: Any) -> 'Expression':
        return InvokeExpression(IdentifierExpression('Value_GT'), [self, other], return_type=FieldType.BOOLEAN)

    @_expression_input
    def __ge__(self, other: Any) -> 'Expression':
        return InvokeExpression(IdentifierExpression('Value_GE'), [self, other], return_type=FieldType.BOOLEAN)

    @_expression_input
    def __add__(self, other: Any) -> 'Expression':
        return InvokeExpression(IdentifierExpression('Add'), [self, other], return_type=FieldType.UNKNOWN)

    @_expression_input
    def __sub__(self, other: Any) -> 'Expression':
        return InvokeExpression(IdentifierExpression('Subtract'), [self, other], return_type=FieldType.UNKNOWN)

    @_expression_input
    def __mul__(self, other: Any) -> 'Expression':
        return InvokeExpression(IdentifierExpression('Multiply'), [self, other], return_type=FieldType.UNKNOWN)

    @_expression_input
    def __truediv__(self, other: Any) -> 'Expression':
        return InvokeExpression(IdentifierExpression('TrueDivide'), [self, other], return_type=FieldType.UNKNOWN)

    @_expression_input
    def __floordiv__(self, other: Any) -> 'Expression':
        return InvokeExpression(IdentifierExpression('FloorDivide'), [self, other], return_type=FieldType.UNKNOWN)

    @_expression_input
    def __mod__(self, other: Any) -> 'Expression':
        return InvokeExpression(IdentifierExpression('Modulo'), [self, other], return_type=FieldType.UNKNOWN)

    @_expression_input
    def __pow__(self, other: Any) -> 'Expression':
        return InvokeExpression(IdentifierExpression('Power'), [self, other], return_type=FieldType.UNKNOWN)

    @_expression_input
    def contains(self, value: Any) -> 'Expression':
        Expression._validate_type(self, FieldType.STRING)
        Expression._validate_type(value, FieldType.STRING)
        return InvokeExpression(IdentifierExpression('String_Contains'), [self, value], return_type=FieldType.BOOLEAN)

    @_expression_input
    def starts_with(self, value: Any) -> 'Expression':
        Expression._validate_type(self, FieldType.STRING)
        Expression._validate_type(value, FieldType.STRING)
        return InvokeExpression(IdentifierExpression('String_StartsWith'), [self, value], return_type=FieldType.BOOLEAN)

    @_expression_input
    def ends_with(self, value: Any) -> 'Expression':
        Expression._validate_type(self, FieldType.STRING)
        Expression._validate_type(value, FieldType.STRING)
        return InvokeExpression(IdentifierExpression('String_EndsWith'), [self, value], return_type=FieldType.BOOLEAN)

    @_expression_input
    def substring(self,
                  start_value: 'IntExpressionLike',
                  length_value: Optional['IntExpressionLike'] = None) -> 'FunctionSupportingExpression':
        Expression._validate_type(self, FieldType.STRING)
        Expression._validate_type(start_value, FieldType.INTEGER)
        Expression._validate_type(length_value, FieldType.INTEGER)
        return InvokeExpression(IdentifierExpression('String_Substring'),
                                [self, start_value, length_value],
                                return_type=FieldType.STRING)

    @_expression_input
    def index_of(self, target: 'StrExpressionLike') -> 'FunctionSupportingExpression':
        Expression._validate_type(self, FieldType.STRING)
        Expression._validate_type(target, FieldType.STRING)
        return InvokeExpression(IdentifierExpression('String_IndexOf'), [self, target], return_type=FieldType.INTEGER)

    @_expression_input
    def length(self) -> 'Expression':
        Expression._validate_type(self, FieldType.STRING or FieldType.LIST)
        return InvokeExpression(IdentifierExpression('Length'),
                                [self],
                                return_type=FieldType.INTEGER)

    @_expression_input
    def to_upper(self) -> 'Expression':
        Expression._validate_type(self, FieldType.STRING)
        return InvokeExpression(IdentifierExpression('ToUpper'),
                                [self],
                                return_type=FieldType.STRING)

    @_expression_input
    def to_lower(self) -> 'Expression':
        Expression._validate_type(self, FieldType.STRING)
        return InvokeExpression(IdentifierExpression('ToLower'),
                                [self],
                                return_type=FieldType.STRING)

    def is_null(self) -> 'Expression':
        return InvokeExpression(IdentifierExpression('Value_IsNull'), [self], return_type=FieldType.BOOLEAN)

    def is_error(self) -> 'Expression':
        return InvokeExpression(IdentifierExpression('Value_IsError'), [self], return_type=FieldType.BOOLEAN)


class IdentifierExpression(FunctionSupportingExpression):
    def __init__(self, identifier: str):
        super().__init__(to_dprep_value({'Identifier': to_dprep_value(identifier)}))


class ValueExpression(FunctionSupportingExpression):
    def __init__(self, value: Any):
        dprep_value, return_type = to_dprep_value_and_type(value)
        super().__init__(to_dprep_value({'Value': dprep_value}), return_type)


class InvokeExpression(FunctionSupportingExpression):
    def __init__(self, function: Expression, arguments: List[Expression], return_type: FieldType = None):
        super().__init__(to_dprep_value({
            'Invoke': [function.underlying_data, [a.underlying_data if a is not None else None for a in arguments]]
        }), return_type=return_type)

    def __getitem__(self, key):
        return col(key, self)


class RecordFieldExpression(FunctionSupportingExpression):
    def __init__(self, record_expression: Expression, field_expression: Expression):
        Expression._validate_type(record_expression, FieldType.DATAROW)
        Expression._validate_type(field_expression, FieldType.STRING)
        super().__init__(to_dprep_value({
            'RecordField': [record_expression.underlying_data, field_expression.underlying_data]
        }))

    def __getitem__(self, key):
        return col(key, self)


class NotExpression(Expression):
    def __init__(self, expression: Expression):
        Expression._validate_type(expression, FieldType.BOOLEAN)
        super().__init__(to_dprep_value({
            'Not': expression.underlying_data
        }), return_type=FieldType.BOOLEAN)


class AndExpression(Expression):
    def __init__(self, lhs: Expression, rhs: Expression):
        Expression._validate_type(lhs, FieldType.BOOLEAN)
        Expression._validate_type(rhs, FieldType.BOOLEAN)
        super().__init__(to_dprep_value({
            'And': [lhs.underlying_data, rhs.underlying_data]
        }), return_type=FieldType.BOOLEAN)


class OrExpression(Expression):
    def __init__(self, lhs: Expression, rhs: Expression):
        Expression._validate_type(lhs, FieldType.BOOLEAN)
        Expression._validate_type(rhs, FieldType.BOOLEAN)
        super().__init__(to_dprep_value({
            'Or': [lhs.underlying_data, rhs.underlying_data]
        }), return_type=FieldType.BOOLEAN)


class IfExpression(FunctionSupportingExpression):
    def __init__(self, condition: Expression, true_value: Expression, false_value: Expression):
        Expression._validate_type(condition, FieldType.BOOLEAN)
        expected_type = true_value.return_type if true_value.return_type == false_value.return_type else None
        super().__init__(to_dprep_value({
            'If': [condition.underlying_data, true_value.underlying_data, false_value.underlying_data]
        }), return_type=expected_type)


class FunctionExpression(Expression):
    def __init__(self, parameters: List[str], members: Dict[str, Expression], expression: Expression):
        super().__init__(to_dprep_value({
            'Function': [
                parameters,
                to_dprep_value({k: v.underlying_data for k, v in members.items()}),
                expression.underlying_data
            ]
        }))


StrExpressionLike = TypeVar('StrExpressionLike', Expression, str)
BoolExpressionLike = TypeVar('BoolExpressionLike', Expression, bool)
IntExpressionLike = TypeVar('IntExpressionLike', Expression, int)  # TODO: Re-add np.int VSO:408215
ExpressionLike = TypeVar('ExpressionLike', StrExpressionLike, IntExpressionLike, BoolExpressionLike)


value = IdentifierExpression('value')


def col(name: StrExpressionLike, record: Expression = None) -> RecordFieldExpression:
    """
    Creates an expression that retrieves the value in the specified column from a record.

    :param name: The name of the column.
    :return: An expression.
    """
    return RecordFieldExpression(record if record is not None else IdentifierExpression('row'), _ensure_expression(name))


def cols(name: List[StrExpressionLike]) -> RecordFieldExpression:
    """
    Creates an expression that retrieves the values in the specified columns from a record.

    :param name: The name of the column.
    :return: An expression.
    """
    return RecordFieldExpression(IdentifierExpression('row'), _ensure_expression(name))


def f_not(expression: Expression) -> Expression:
    """
    Negates the specified expression.

    :param expression: An expression.
    :return: The negated expression.
    """
    return NotExpression(expression)


def f_and(*expressions: List[Expression]) -> Expression:
    """
    Returns an expression that evaluates to true if all expressions are true; false otherwise. This expression
        supports short-circuit evaluation.

    :param expressions: List of expressions, at least 2 expressions are required.
    :return: An expression that results in a boolean value.
    """
    return _reduce_bin_exp(expressions, AndExpression)


def f_or(*expressions: List[Expression]) -> Expression:
    """
    Returns an expression that evaluates to true if any expression is true; false otherwise. This expression
        supports short-circuit evaluation.

    :param expressions: List of expressions, at least 2 expressions are required.
    :return: An expression that results in a boolean value.
    """
    return _reduce_bin_exp(expressions, OrExpression)


def cond(condition: Expression, if_true: Any, or_else: Any) -> Expression:
    """
    Returns a conditional expression that will evaluate an input expression and return one value/expression if it
        evaluates to true or a different one if it doesn't.

    :param condition: The expression to evaluate.
    :param if_true: The value/expression to use if the expression evaluates to True.
    :param or_else: The value/expression to use if the expression evaluates to False.
    :return: A conditional expression.
    """
    true_value = _ensure_expression(if_true)
    false_value = _ensure_expression(or_else)
    return IfExpression(condition, true_value, false_value)


def _reduce_bin_exp(expressions: List[Expression], expression_ctr: Any) -> Expression:
    if len(expressions) < 2:
        raise ValueError('There need to be at least two expressions, only received {}'.format(len(expressions)))

    prev_exp = expressions[0]
    for exp in expressions[1:]:
        prev_exp = expression_ctr(prev_exp, exp)
    return prev_exp
