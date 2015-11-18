# Skeleton of a CLI

import click

import pgdb


@click.command('pgdb')
@click.argument('count', type=int, metavar='N')
def cli(count):
    """Echo a value `N` number of times"""

