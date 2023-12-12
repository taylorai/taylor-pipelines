import abc
import re
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
            print("Warning: required argument has default value. This should only be done as guidance for the user.")
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

    @abc.abstractmethod
    def to_json(self) -> dict:
        """
        Returns a JSON representation of the argument.
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
        default: Optional[Any] = None,
        value: Optional[Any] = None,
    ):
        super().__init__(name, description, required, default, value)
        self.choices = choices
        self.type = str

    def validate(self) -> bool:
        """
        Validates a value for the argument.
        """
        return self.value in self.choices

    def to_json(self) -> dict:
        """
        Returns a JSON representation of the argument.
        """
        return {
            "name": self.name,
            "description": self.description,
            "required": self.required,
            "default": self.default,
            "value": self.value,
            "choices": self.choices,
            "type": "MultipleChoiceArgument",
        }


class StringArgument(Argument):
    """
    A string argument is an argument that takes a string value.
    """
    valid_regex: str = None

    def __init__(
        self,
        name: str,
        description: str,
        required: bool = True,
        default: Optional[Any] = None,
        value: Optional[Any] = None,
        valid_regex: Optional[str] = None,
    ):
        super().__init__(name, description, required, default, value)
        self.valid_regex = valid_regex
        self.type = str

    def validate(self, value: Any) -> bool:
        """
        Validates a value for the argument.
        """
        if not isinstance(value, str):
            return False
        if self.valid_regex is not None:
            return re.match(self.valid_regex, value) is not None
        return True
    
    def to_json(self) -> dict:
        """
        Returns a JSON representation of the argument.
        """
        return {
            "name": self.name,
            "description": self.description,
            "required": self.required,
            "default": self.default,
            "value": self.value,
            "valid_regex": self.valid_regex,
            "type": "StringArgument",
        }
    
class ListArgument(Argument):
    """
    A list argument is a list of strings (for now).
    """
    valid_regex: str = None

    def __init__(
        self,
        name: str,
        description: str,
        required: bool = True,
        default: Optional[Any] = None,
        value: Optional[Any] = None,
        valid_regex: Optional[str] = None,
    ):
        super().__init__(name, description, required, default, value)
        self.valid_regex = valid_regex
        self.type = list[str]

    def validate(self, value: Any) -> bool:
        """
        Validates a value for the argument.
        """
        if not isinstance(value, list):
            return False
        for item in value:
            if not isinstance(item, str):
                return False
            if self.valid_regex is not None:
                if re.match(self.valid_regex, item) is None:
                    return False
        return True
    
    def to_json(self) -> dict:
        """
        Returns a JSON representation of the argument.
        """
        return {
            "name": self.name,
            "description": self.description,
            "required": self.required,
            "default": self.default,
            "value": self.value,
            "valid_regex": self.valid_regex,
            "type": "ListArgument",
        }


class IntegerArgument(Argument):
    """
    An integer argument is an argument that takes an integer value.
    """

    def __init__(
        self,
        name: str,
        description: str,
        required: bool = True,
        default: Optional[Any] = None,
        value: Optional[Any] = None,
        min_value: int = None,
        max_value: int = None,
    ):
        super().__init__(name, description, required, default, value)
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

    def to_json(self) -> dict:
        """
        Returns a JSON representation of the argument.
        """
        return {
            "name": self.name,
            "description": self.description,
            "required": self.required,
            "default": self.default,
            "value": self.value,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "type": "IntegerArgument",
        }

class BooleanArgument(Argument):
    """
    A boolean argument is an argument that takes a boolean value.
    """
    def __init__(
        self,
        name: str,
        description: str,
        required: bool = True,
        default: Optional[Any] = False,
        value: Optional[Any] = None,
    ):
        super().__init__(name, description, required, default, value)
        self.type = bool

    def validate(self) -> bool:
        """
        Validates a value for the argument.
        """
        return isinstance(self.value, bool)

    def to_json(self) -> dict:
        """
        Returns a JSON representation of the argument.
        """
        return {
            "name": self.name,
            "description": self.description,
            "required": self.required,
            "default": self.default,
            "value": self.value,
            "type": "BooleanArgument",
        }

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
