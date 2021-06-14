from invoke import task

@task
def test(context):
    """notes"""
    from app.tasks.db import build

    build.test_build()
    
    return
