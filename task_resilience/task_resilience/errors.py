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


class InvalidRequest(ValueError):
    """The request cannot be accepted as submitted."""


class IdempotencyConflict(Exception):
    """A request ID was replayed with different contents."""

    def __init__(self, request_id: str) -> None:
        super().__init__(
            f'request_id {request_id!r} was already accepted with different contents')
        self.request_id = request_id


class MissionNotFound(Exception):
    """No mission exists for a mission ID or request ID."""

    def __init__(self, identifier: str) -> None:
        super().__init__(f'no mission for {identifier!r}')
        self.identifier = identifier


class AttemptLimitReached(Exception):
    """Every accepted attempt for the mission has already been used."""

    def __init__(self, mission_id: str, max_attempts: int) -> None:
        super().__init__(f'mission {mission_id!r} has used all {max_attempts} attempts')
        self.mission_id = mission_id
        self.max_attempts = max_attempts


class UnsupportedSchemaVersion(Exception):
    """The database was written by a schema version this code does not understand."""

    def __init__(self, version: int) -> None:
        super().__init__(f'unsupported database schema version {version}')
        self.version = version
