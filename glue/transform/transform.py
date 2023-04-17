from glue_job_libs import job_handler
from glue_job_libs.transform_base_config import TransformConfig
from appsflyer import transformer


config = TransformConfig()

parser_class_map = {
    'DataParser': transformer.DataParser
}

job_handler.main(config, parser_class_map)
