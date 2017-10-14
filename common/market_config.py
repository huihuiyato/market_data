import ConfigParser
import argparse
from common import singleton

@singleton
class MarketConfigEntity:
    config_parser = None

    def get_parser(self):
        if not self.config_parser:
            cmd_args_parser = argparse.ArgumentParser()
            cmd_args_parser.add_argument('--config', dest = 'config', required = True)
            args, _ = cmd_args_parser.parse_known_args()
            self.config_parser = ConfigParser.ConfigParser()
            self.config_parser.read(args.config)
        return self.config_parser

class MarketConfig:
    @staticmethod
    def get(section, name, is_interpolated = 0):
        entity = MarketConfigEntity()
        config_parser = entity.get_parser()
        return config_parser.get(section, name, is_interpolated)
