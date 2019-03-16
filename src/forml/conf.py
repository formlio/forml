"""ForML configuration
"""
import argparse
import configparser
import os.path

APP_NAME = 'forml'
USR_DIR = os.getenv(f'{APP_NAME.upper()}_HOME', os.path.join(os.path.expanduser('~'), f'.{APP_NAME}'))
SYS_DIR = os.path.join('/etc', APP_NAME)

SECTION_DEFAULT = 'DEFAULT'
OPTION_LOG_CFGFILE = 'log_cfgfile'

DEFAULT_OPTIONS = {
    OPTION_LOG_CFGFILE: 'logging.ini',
}

CLI_PARSER = argparse.ArgumentParser()
CLI_PARSER.add_argument('-C', '--config', type=argparse.FileType(), help='Config file')
CLI_ARGS, _ = CLI_PARSER.parse_known_args()

APP_CFGFILE = CLI_ARGS.config or 'config.ini'
CONFIG = configparser.ConfigParser(DEFAULT_OPTIONS)
CONFIG.optionxform = str   # we need case sensitivity
USED_CONFIGS = CONFIG.read((os.path.join(d, APP_CFGFILE) for d in (SYS_DIR, USR_DIR)))

LOG_CFGFILE = CONFIG.get(SECTION_DEFAULT, OPTION_LOG_CFGFILE)

#REGISTRY