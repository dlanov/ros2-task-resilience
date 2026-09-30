# Copyright 2026 Dennis Lanov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pathlib import Path

import pytest

# The core tests run without ROS; the lint tests need the ament tools.
main = pytest.importorskip('ament_copyright.main').main

PACKAGE_DIR = Path(__file__).resolve().parent.parent


@pytest.mark.copyright
@pytest.mark.linter
def test_copyright():
    # Lint this package only, whatever directory pytest was started from.
    rc = main(argv=[str(PACKAGE_DIR)])
    assert rc == 0, 'Found errors'
