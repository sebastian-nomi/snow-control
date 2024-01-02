from colorama import Fore, Style
from typing import Callable
import time 
import os
from getpass import getpass

CHECKMARK = '\u2714'
RED_X = ' X '
STYLING = {
    'bright': Style.BRIGHT, 
    'dim':Style.DIM,
    'normal': Style.NORMAL, 
    'end': Style.RESET_ALL, 
    'cyan':Fore.CYAN, 
    'yellow':Fore.YELLOW,
    'green':Fore.GREEN,
    'red':Fore.RED,
    'ORGANIZATION':os.environ['SNOWFLAKE_ORGANIZATION']
}

def time_func(func: Callable) -> Callable: 
    def inner(*args, **kwargs): 
        start = time.time()
        return_val = func(*args, **kwargs)
        print(f'\n{Style.BRIGHT}Function {func.__name__} took {Fore.YELLOW}{time.time()-start:.2f} {Style.RESET_ALL + Style.BRIGHT} seconds')
        return return_val
    return inner

def cli_input(input_text):
    # Make input text clearly distinct
    PASSWORD_KEYWORD = '[password]'
    if PASSWORD_KEYWORD in input_text: 
        cleaned_prompt = input_text.replace(PASSWORD_KEYWORD, '')
        output = getpass(cleaned_prompt + Style.RESET_ALL + Fore.YELLOW)
    else:
        output = input(input_text + Style.RESET_ALL + Fore.YELLOW) 
    return output.strip()

def format_privilege(privilege:str, object_type:str, object_name:str, delta:str) -> str:
    """
        Evenly space the privilege (4,40,20) chars so it can be more cleanly run 
    """
    return f"{delta:4} {privilege:40} {object_type:20} {object_name}"

def print_formatted_plan(plan:dict, grants_to = 'ROLE', verbosity = 3) -> None: 
    TABLE_FLIP = '(╯°□°)╯︵ ┻━┻'
    print('\n'*4)
    for recipient, config in plan.items(): 
        # SKIP if all good!
        if not config['to_revoke'] and not config['to_grant']: 
            if verbosity >= 3: 
                print(f'{Style.BRIGHT + Fore.YELLOW}{grants_to}: {recipient}')
                print(f'{Style.BRIGHT + Fore.CYAN}ALL_GOOD!:({len(config["ok"])}:0) {TABLE_FLIP}', end = '\n\n')
            continue
        print(f'{Style.BRIGHT + Fore.YELLOW}{grants_to}: {recipient}')

        if config['to_revoke'] or verbosity >=3: 
            print(f'{Style.BRIGHT+Fore.CYAN}PRIVILEGES TO BE {Style.BRIGHT + Fore.RED}REVOKED:', end = '\n\n')
            for minus in sorted(config['to_revoke'], key = lambda x: x[1] + x[2] + x[3]): 
                print(Fore.RED + format_privilege(*minus, delta = '-'))
            print('\n')
        if config['to_grant'] or verbosity >=3: 
            print(f'{Style.BRIGHT+Fore.CYAN}PRIVILEGES TO BE {Style.BRIGHT + Fore.GREEN}GRANTED:', end = '\n\n')
            for minus in sorted(config['to_grant'], key = lambda x: x[1] + x[2] + x[3]): 
                print(Fore.GREEN + format_privilege(*minus, delta = '-'))
            print('\n')
        if verbosity >=2: 
            print(f'{Style.BRIGHT}Grant Deltas: {recipient}')
            print(f'{Style.BRIGHT+Fore.RED}- {len(config["to_revoke"])}')
            print(f'{Style.BRIGHT+Fore.CYAN}= {len(config["ok"])}')
            print(f'{Style.BRIGHT+Fore.GREEN}+ {len(config["to_grant"])}')
            print('==================')
            print(f'{Style.BRIGHT}T {len(config["to_revoke"]) + len(config["ok"]) + len(config["to_grant"])}')

def show(queries:list) -> None:
    for q in queries:
        print(format_grant('+' if q[0] == 'GRANT' else '-', *q))

def format_grant(delta, *grant): 
    """
        format_grant turns a grant( privilege, delta, target_role)
        into a pretty-printable color statement
    """

    assert delta in ('+','-')
    style = Fore.RED if delta == '-' else Fore.GREEN
    format_string = str(style) + delta + "\t"+" ".join(grant)
    return format_string