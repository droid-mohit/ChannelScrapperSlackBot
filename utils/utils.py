import logging

logger = logging.getLogger(__name__)


def clean_string(input_str: str, change_case_lower: bool = True, change_case_upper: bool = False,
                 clear_trailing_spaces: bool = True):
    if change_case_lower and change_case_upper:
        logger.error("Only one of change_case_lower or change_case_upper can be True")
        return input_str
    if change_case_lower and not change_case_upper:
        input_str = input_str.lower()
    if change_case_upper and not change_case_lower:
        input_str = input_str.upper()
    if clear_trailing_spaces:
        input_str = input_str.strip()
    return input_str
