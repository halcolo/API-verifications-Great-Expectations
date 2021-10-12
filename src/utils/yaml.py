import yaml
from yaml.loader import SafeLoader, FullLoader
from utils.loggin_messages import error_messages, info_messages
from operator import attrgetter

class Yaml():
    def __init__(self, yaml_route, source='Local'):
        self.yaml_route = yaml_route
        self.source = source
        self.yaml_decoded = None
        self.yaml_encoded = None
        self.sources = {
            'Local': self.get_local,
            'GoogleCloudBucket': self.get_gcs
        }
        self.evaluate_source()
    
    def get_yaml_encoded(self):
        return self.yaml_encoded
        
    def get_yaml_decoded(self):
        return self.yaml_decoded
    
    def evaluate_source(self):
        if self.source.capitalize() in self.sources:
            self.sources[self.source.capitalize()]()
        # function = lambda self, x : self.sources.get(x, lambda x : self.not_founded_source())(self)
        # unction(self, self.source)
    
    def not_founded_source(self):
        message="source added:{}".format(self.source)
        info_messages(auto=False, message=message)
        error_messages(1)

    def get_local(self):
        with open(self.yaml_route) as text:
            self.decoding_yaml(text)

    def get_gcs(self):
        pass

    def decoding_yaml(self, text):
        # Decoded is the yaml file translated as python Dict
        self.yaml_decoded = yaml.load(text, Loader=SafeLoader)
        self.yaml_encoded = yaml.dump(self.yaml_decoded, default_flow_style=False)
        info_messages(auto=False, message=self.yaml_decoded)

