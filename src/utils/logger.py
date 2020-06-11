# Copyright (C) 2020 University of Oxford
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import logging
from utils.config import config


def setup_logger():
    format = '%(asctime)s %(levelname)s %(name)s %(message)s'
    level = config.LOGLEVEL
    log_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'fetcher.log')

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.WARNING)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    handlers = [fh, logging.StreamHandler()]
    logging.basicConfig(level=level, format=format, handlers=handlers)
