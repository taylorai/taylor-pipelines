import abc
from typing import Any, Optional


class Argument(abc.ABC):
    """
    An argument is a parameter that can be passed to a pipeline operation.
    It corresponds to a CLI argument if running from the command line, or
    a UI element if running from the UI.
    """

    name: str
    description: str
    required: bool = True
    default: Optional[Any] = None
    value: Optional[Any] = None

    def __init__(
        self,
        name: str,
        description: str,
        required: bool = True,
        default: Optional[Any] = None,
        value: Optional[Any] = None,
    ):
        if required and default:
            raise ValueError(
                "An argument cannot be both required and have a default value."
            )
        self.name = name
        self.description = description
        self.required = required
        self.default = default
        self.value = value
        self.type = type(default)

    def set_value(self, value: Any):
        """
        Sets the value of the argument.
        """
        self.value = value
        if not self.validate():
            raise ValueError(f"Invalid value for {self.name}: {value}")

    def validate(self) -> bool:
        """
        Validates a value for the argument.
        """
        raise NotImplementedError


class MultipleChoiceArgument(Argument):
    """
    A multiple choice argument is an argument that can take one of a set of
    string values.
    """

    choices: list[str]

    def __init__(
        self,
        name: str,
        description: str,
        choices: list[str],
        required: bool = True,
        value: Optional[Any] = None,
    ):
        super().__init__(name, description, required, value)
        self.choices = choices
        self.type = str

    def validate(self) -> bool:
        """
        Validates a value for the argument.
        """
        return self.value in self.choices


# class StringArgument(Argument):
#     """
#     A string argument is an argument that takes a string value.
#     """
#     valid_regex: str = None

#     def validate(self, value: Any) -> bool:
#         """
#         Validates a value for the argument.
#         """
#         if not isinstance(value, str):
#             return False
#         if self.valid_regex is not None:
#             return re.match(self.valid_regex, value) is not None
#         return True


class IntegerArgument(Argument):
    """
    An integer argument is an argument that takes an integer value.
    """

    def __init__(
        self,
        name: str,
        description: str,
        required: bool = True,
        value: Optional[Any] = None,
        min_value: int = None,
        max_value: int = None,
    ):
        super().__init__(name, description, required, value)
        self.min_value = min_value
        self.max_value = max_value
        self.type = int

    def validate(self) -> bool:
        """
        Validates a value for the argument.
        """
        if not isinstance(self.value, int):
            return False
        if self.min_value is not None and self.value < self.min_value:
            return False
        if self.max_value is not None and self.value > self.max_value:
            return False
        return True


# @dataclass
# class BooleanArgument(Argument):
#     """
#     A boolean argument is an argument that takes a boolean value.
#     """
#     def validate(self, value: Any) -> bool:
#         """
#         Validates a value for the argument.
#         """
#         return isinstance(value, bool)

# @dataclass
# class FloatArgument(Argument):
#     """
#     A float argument is an argument that takes a float value.
#     """
#     min_value: float = None
#     max_value: float = None

#     def validate(self, value: Any) -> bool:
#         """
#         Validates a value for the argument.
#         """
#         if not isinstance(value, float):
#             return False
#         if self.min_value is not None and value < self.min_value:
#             return False
#         if self.max_value is not None and value > self.max_value:
#             return False
#         return True

# @dataclass
# class DateArgument(Argument):
#     """
#     A date argument is an argument that takes a date value.
#     """
#     min_value: str = None
#     max_value: str = None

#     def validate(self, value: Any) -> bool:
#         """
#         Validates a value for the argument.
#         """
#         if not isinstance(value, str):
#             return False
#         if self.min_value is not None and value < self.min_value:
#             return False
#         if self.max_value is not None and value > self.max_value:
#             return False
#         return True
