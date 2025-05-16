import os
import time
from getpass import getpass
from typing import Callable

from colorama import Fore, Style

CHECKMARK = "\u2714"
GREEN_CHECKMARK = Fore.GREEN + Style.BRIGHT + CHECKMARK + Style.RESET_ALL
RED_X = Fore.RED + Style.BRIGHT + " X " + Style.RESET_ALL
STYLING = {
    "bright": Style.BRIGHT,
    "dim": Style.DIM,
    "normal": Style.NORMAL,
    "end": Style.RESET_ALL,
    "cyan": Fore.CYAN,
    "yellow": Fore.YELLOW,
    "green": Fore.GREEN,
    "red": Fore.RED,
    "ORGANIZATION": os.environ["SNOWFLAKE_ORGANIZATION"],
}


def time_func(func: Callable) -> Callable:
    def inner(*args, **kwargs):
        start = time.time()
        return_val = func(*args, **kwargs)
        print(
            f"\n{Style.BRIGHT}Function {func.__name__} took {Fore.YELLOW}{time.time()-start:.2f} {Style.RESET_ALL + Style.BRIGHT} seconds"
        )
        return return_val

    return inner


def cli_input(input_text):
    # Make input text clearly distinct
    PASSWORD_KEYWORD = "[password]"
    if PASSWORD_KEYWORD in input_text:
        cleaned_prompt = input_text.replace(PASSWORD_KEYWORD, "")
        output = getpass(cleaned_prompt + Style.RESET_ALL + Fore.YELLOW)
    else:
        output = input(input_text + Style.RESET_ALL + Fore.YELLOW)
    return output.strip()


def format_privilege(
    privilege: str, object_type: str, object_name: str, delta: str
) -> str:
    """
    Evenly space the privilege (4,40,20) chars so it can be more cleanly run
    """
    return f"{delta:4} {privilege:40} {object_type:20} {object_name}"


def show(queries: list) -> None:
    for q in queries:
        print(format_grant("+" if q[0] == "GRANT" else "-", *q))


def print_execution(executable: str, success=None, end="\n"):
    if success == "+":
        print(f"{Fore.CYAN}{executable}{GREEN_CHECKMARK}", end=end)
    elif success == "-":
        print(f"{Fore.RED}{executable}{RED_X}", end=end)
    else:
        print(f"{Fore.CYAN}{executable}", end=end)


def format_grant(delta, *grant):
    """
    format_grant turns a grant( privilege, delta, target_role)
    into a pretty-printable color statement
    """

    assert delta in ("+", "-")
    style = Fore.RED if delta == "-" else Fore.GREEN
    format_string = str(style) + delta + "\t" + " ".join(grant)
    return format_string
