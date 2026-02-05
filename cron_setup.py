import subprocess
from scheduler import install_meta_cron, generate_and_install


def _remove_old_cron():
    """Remove the legacy Sunday-noon cron entry (untagged, from before the scheduler)."""
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if result.returncode != 0:
        return
    existing = result.stdout
    lines = [l for l in existing.splitlines()
             if not (l.strip() and "main.py" in l and "ARMITAGE" not in l)]
    new_crontab = "\n".join(lines).strip() + "\n" if lines else ""
    subprocess.run(["crontab", "-"], input=new_crontab, text=True, check=True)


def setup():
    """Install the weekly scheduler meta-cron, replacing the old single cron."""
    _remove_old_cron()
    install_meta_cron()
    generate_and_install()


if __name__ == "__main__":
    setup()
