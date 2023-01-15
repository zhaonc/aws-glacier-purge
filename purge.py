import boto3
import click
import logging
import time
import json
from typing import Optional

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


@click.group()
@click.option("--vault", help="Vault name", required=True)
@click.option("--account-id", help="Account id", required=False)
@click.pass_context
def cli(ctx, vault: str, account_id: Optional[str] = None):
    ctx.ensure_object(dict)
    ctx.obj["vault"] = vault
    ctx.obj["account_id"] = account_id

    glacier = boto3.client("glacier")
    ctx.obj["glacier"] = glacier


@cli.command()
@click.option("--block", is_flag=True)
@click.pass_context
def init(ctx, block=False):
    vault = ctx.obj["vault"]
    account_id = ctx.obj["account_id"] or None
    glacier = ctx.obj["glacier"]
    actually_init(vault, account_id, glacier, block)


def actually_init(vault, account_id, glacier, block=False):
    logger.info("Initiating Glacier job")
    res = glacier.initiate_job(
        vaultName=vault,
        accountId=account_id,
        jobParameters={"Type": "inventory-retrieval"},
    )
    job_id = res["jobId"]
    logger.info(f"Job id = {job_id}")

    while block:
        logger.info("Waiting for 10 seconds")
        time.sleep(10)
        block = not actually_check(vault, account_id, glacier, job_id)

    return job_id


@cli.command()
@click.option("--job_id")
@click.pass_context
def check(ctx, job_id):
    vault = ctx.obj["vault"]
    account_id = ctx.obj["account_id"] or None
    glacier = ctx.obj["glacier"]
    actually_check(vault, account_id, glacier, job_id)


def actually_check(vault, account_id, glacier, job_id) -> bool:
    res = glacier.describe_job(vaultName=vault, accountId=account_id, jobId=job_id)
    status = res["StatusCode"]
    if status == "Succeeded":
        logger.info("Successfully retrieved job")
        return True
    elif status == "Failed":
        logger.error("Glacier job failed")
        logger.error(res)
        return True
    else:
        return False


@cli.command()
@click.option("--job_id")
@click.pass_context
def delete_archives(ctx, job_id):
    vault = ctx.obj["vault"]
    account_id = ctx.obj["account_id"] or None
    glacier = ctx.obj["glacier"]
    actually_delete_archives(vault, account_id, glacier, job_id)


def actually_delete_archives(vault, account_id, glacier, job_id):
    logger.info("Retrieving archiveId")
    res = glacier.get_job_output(vaultName=vault, accountId=account_id, jobId=job_id)
    body = json.load(res["body"])
    archive_list = body["ArchiveList"]
    logger.info(f"Archives: {json.dumps(archive_list, indent=4)}")

    for archive in archive_list:
        archive_id = archive["ArchiveId"]
        logger.info(f"Deleting archive {archive_id}")
        glacier.delete_archive(vaultName=vault, archiveId=archive_id)


@cli.command()
@click.pass_context
def delete_vault(ctx):
    vault = ctx.obj["vault"]
    glacier = ctx.obj["glacier"]
    actually_delete_vault(vault, glacier)


def actually_delete_vault(vault, glacier):
    logger.info("Deleting vault")
    glacier.delete_vault(vaultName=vault)


@cli.command(help="Run everything automatically")
@click.pass_context
def purge(ctx):
    vault = ctx.obj["vault"]
    account_id = ctx.obj["account_id"] or None
    glacier = ctx.obj["glacier"]

    job_id = actually_init(vault, account_id, glacier, block=True)
    actually_delete_archives(vault, account_id, glacier, job_id)
    job_id = actually_init(vault, account_id, glacier, block=True)
    actually_delete_vault(vault, glacier)


if __name__ == "__main__":
    cli(obj={})
