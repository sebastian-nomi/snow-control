from concurrent.futures import ThreadPoolExecutor
from colorama import Style,Fore
from styling import * 

class ControlState:
    __slots__ = 'connection', 'account', 'executor','snowcache','snowplan','queries','ignore_objects','verbosity'
    def __init__(self, max_workers = 100):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.verbosity = 0 

    def __del__(self): 
        self.executor.shutdown()
    
    def print(self,message,  verbosity_level = 0, **kwargs):
        if verbosity_level <= self.verbosity:
            print(message, **kwargs)

    
    def print_formatted_plan(self, plan:dict, grants_to = 'ROLE') -> None: 
        TABLE_FLIP = '(╯°□°)╯︵ ┻━┻'
        self.print('\n'*4)
        for recipient, config in plan.items(): 
            # SKIP if all good!
            if not config['to_revoke'] and not config['to_grant']: 
                self.print(f'{Style.BRIGHT + Fore.YELLOW}{grants_to}: {recipient}',verbosity_level=3)
                self.print(f'{Style.BRIGHT + Fore.CYAN}ALL_GOOD!:({len(config["ok"])}:0) {TABLE_FLIP}', verbosity_level=3, end = '\n\n')
            self.print(f'{Style.BRIGHT + Fore.YELLOW}{grants_to}: {recipient}')

            if config['to_revoke'] or self.verbosity >= 3: 
                self.print(f'{Style.BRIGHT+Fore.CYAN}PRIVILEGES TO BE {Style.BRIGHT + Fore.RED}REVOKED:', end = '\n\n')
                for minus in sorted(config['to_revoke'], key = lambda x: x[1] + x[2] + x[0]): 
                    self.print(Fore.RED + format_privilege(*minus, delta = '-'))
                self.print('\n')
            if config['to_grant'] or self.verbosity >=3: 
                self.print(f'{Style.BRIGHT+Fore.CYAN}PRIVILEGES TO BE {Style.BRIGHT + Fore.GREEN}GRANTED:', end = '\n\n')
                for minus in sorted(config['to_grant'], key = lambda x: x[1] + x[2] + x[0]): 
                    self.print(Fore.GREEN + format_privilege(*minus, delta = '-'))
                self.print('\n')
            self.print(f'{Style.BRIGHT}Grant Deltas: {recipient}', verbosity_level= 2 )
            self.print(f'{Style.BRIGHT+Fore.RED}- {len(config["to_revoke"])}', verbosity_level= 2)
            self.print(f'{Style.BRIGHT+Fore.CYAN}= {len(config["ok"])}', verbosity_level= 2)
            self.print(f'{Style.BRIGHT+Fore.GREEN}+ {len(config["to_grant"])}', verbosity_level= 2)
            self.print('==================', verbosity_level= 2)
            self.print(f'{Style.BRIGHT}T {len(config["to_revoke"]) + len(config["ok"]) + len(config["to_grant"])}', verbosity_level= 2)
