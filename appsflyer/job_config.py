
from glue_job_libs import parameter_store
from glue_job_libs.base_config import BaseConfig



class JobConfig(BaseConfig):
    def __init__(self) -> None:
        super().__init__()

    @property
    def landing_dir(self):
        return "landing"


    def get_access_token(self):
        parameter_name = 'token'

        if parameter_name != '':
            ps = parameter_store.ParameterStore()
            parameter_name = f'/{self.environment}/{self.data_source}/{parameter_name}'
            result =  ps.get_encrypted_parameter_value(parameter_name=parameter_name, decrypt=True)

        else:
            result = ''

        return result

