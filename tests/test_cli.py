from __future__ import absolute_import
import os

from click.testing import CliRunner

import pgdata
from pgdata.cli import cli


URL = "postgresql://postgres:postgres@localhost:5432/pgdata"
DATASET = 'bc-airports'
EMAIL = os.environ["BCDATA_EMAIL"]


def test_bc2pg():
    runner = CliRunner()
    result = runner.invoke(cli,
                           [DATASET, '--email', EMAIL, '--db_url', URL])
    assert result.exit_code == 0
    db = pgdata.connect(url=URL)
    assert 'whse_imagery_and_base_maps' in db.schemas
    assert 'whse_imagery_and_base_maps.gsr_airports_svw' in db.tables
