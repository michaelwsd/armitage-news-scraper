import os
import sys
import json
import fcntl
import asyncio
import random
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

from scraper import read_companies_from_csv, scrape_companies
from email_client import send_digest_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Paths & constants
# -------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent
SCHEDULE_DIR = PROJECT_DIR / "data" / "schedule"
SCHEDULE_FILE = SCHEDULE_DIR / "weekly_schedule.json"
PYTHON_PATH = PROJECT_DIR / ".venv" / "bin" / "python"
CRON_LOG = PROJECT_DIR / "cron.log"

CRON_SESSION_TAG = "# ARMITAGE_SESSION"
CRON_META_TAG = "# ARMITAGE_META"

MIN_GROUP_SIZE = 1
MAX_GROUP_SIZE = 4
SCRAPE_HOUR_START = 9   # earliest session hour (inclusive)
SCRAPE_HOUR_END = 21    # latest session hour (exclusive)
INTER_COMPANY_DELAY_MIN = 300   # 5 minutes
INTER_COMPANY_DELAY_MAX = 900   # 15 minutes

DAY_NAMES = {1: "Monday", 2: "Tuesday", 3: "Wednesday",
             4: "Thursday", 5: "Friday", 6: "Saturday"}


# -------------------------------------------------------------------
# Schedule file I/O (with file locking)
# -------------------------------------------------------------------

def _load_schedule():
    if not SCHEDULE_FILE.exists():
        return None
    with open(SCHEDULE_FILE, "r") as f:
        return json.load(f)


def _save_schedule(schedule):
    SCHEDULE_DIR.mkdir(parents=True, exist_ok=True)
    with open(SCHEDULE_FILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        json.dump(schedule, f, indent=2)
        fcntl.flock(f, fcntl.LOCK_UN)


# -------------------------------------------------------------------
# Schedule generation
# -------------------------------------------------------------------

def _partition_companies(companies):
    """Shuffle and split companies into random groups of 1-4."""
    shuffled = list(companies)
    random.shuffle(shuffled)

    groups = []
    i = 0
    while i < len(shuffled):
        remaining = len(shuffled) - i
        max_take = min(MAX_GROUP_SIZE, remaining)
        group_size = random.randint(MIN_GROUP_SIZE, max_take)
        groups.append(shuffled[i:i + group_size])
        i += group_size
    return groups


def _assign_schedule_slots(num_sessions):
    """Assign (cron_day, hour, minute) to each session, spread across Mon-Sat."""
    available_days = list(range(1, 7))  # cron: 1=Mon ... 6=Sat

    # Distribute sessions across days
    if num_sessions <= len(available_days):
        day_assignments = random.sample(available_days, num_sessions)
    else:
        day_assignments = []
        base, remainder = divmod(num_sessions, len(available_days))
        for day in available_days:
            count = base + (1 if remainder > 0 else 0)
            if remainder > 0:
                remainder -= 1
            day_assignments.extend([day] * count)
        random.shuffle(day_assignments)

    # Assign times, avoiding same (day, hour) collisions
    used_slots = set()
    slots = []
    for day in day_assignments:
        hour = None
        for _ in range(50):
            h = random.randint(SCRAPE_HOUR_START, SCRAPE_HOUR_END - 1)
            if (day, h) not in used_slots:
                hour = h
                used_slots.add((day, h))
                break
        if hour is None:
            hour = random.randint(SCRAPE_HOUR_START, SCRAPE_HOUR_END - 1)
        minute = random.randint(0, 59)
        slots.append((day, hour, minute))

    return slots


def generate_weekly_schedule(companies):
    """Generate a full weekly schedule covering all companies."""
    groups = _partition_companies(companies)
    slots = _assign_schedule_slots(len(groups))

    # Compute the upcoming Monday as "week_of"
    today = datetime.now()
    days_until_monday = (7 - today.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    next_monday = today + timedelta(days=days_until_monday)

    sessions = []
    for idx, (group, (day, hour, minute)) in enumerate(zip(groups, slots)):
        # day is cron day-of-week: 1=Mon ... 6=Sat
        # next_monday is weekday 0 (Mon), so offset = day - 1
        session_date = next_monday + timedelta(days=day - 1)
        sessions.append({
            "session_id": f"sess_{idx:03d}",
            "day": day,
            "day_name": DAY_NAMES[day],
            "date": session_date.strftime("%Y-%m-%d"),
            "hour": hour,
            "minute": minute,
            "companies": [list(c) for c in group],
            "status": "pending",
            "started_at": None,
            "completed_at": None,
        })

    # Sort by (day, hour, minute) for readability
    sessions.sort(key=lambda s: (s["day"], s["hour"], s["minute"]))

    schedule = {
        "week_of": next_monday.strftime("%Y-%m-%d"),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_companies": len(companies),
        "digest_sent": False,
        "sessions": sessions,
    }
    return schedule


# -------------------------------------------------------------------
# Crontab management
# -------------------------------------------------------------------

def _read_crontab():
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    return result.stdout if result.returncode == 0 else ""


def _write_crontab(content):
    subprocess.run(["crontab", "-"], input=content, text=True, check=True)


def install_session_crons(schedule):
    """Install one cron entry per session, removing old session crons first."""
    existing = _read_crontab()

    # Remove old session crons
    lines = [l for l in existing.splitlines() if CRON_SESSION_TAG not in l]

    # Add new session crons
    for session in schedule["sessions"]:
        cron_line = (
            f"{session['minute']} {session['hour']} * * {session['day']} "
            f"cd {PROJECT_DIR} && {PYTHON_PATH} scheduler.py run-session {session['session_id']} "
            f">> {CRON_LOG} 2>&1 {CRON_SESSION_TAG}"
        )
        lines.append(cron_line)

    new_crontab = "\n".join(lines).strip() + "\n"
    _write_crontab(new_crontab)
    logger.info(f"Installed {len(schedule['sessions'])} session cron entries")


def install_meta_cron():
    """Install the weekly meta cron (Sunday 22:00) if not already present."""
    existing = _read_crontab()

    if CRON_META_TAG in existing:
        logger.info("Meta cron already installed")
        return

    meta_line = (
        f"0 22 * * 0 cd {PROJECT_DIR} && {PYTHON_PATH} scheduler.py generate "
        f">> {CRON_LOG} 2>&1 {CRON_META_TAG}"
    )
    new_crontab = existing.rstrip("\n") + "\n" + meta_line + "\n" if existing.strip() else meta_line + "\n"
    _write_crontab(new_crontab)
    logger.info("Meta cron installed (Sunday 22:00)")


def remove_session_crons():
    """Remove all session cron entries."""
    existing = _read_crontab()
    lines = [l for l in existing.splitlines() if CRON_SESSION_TAG not in l]
    new_crontab = "\n".join(lines).strip() + "\n" if lines else ""
    _write_crontab(new_crontab)
    logger.info("Session crons removed")


def remove_all_armitage_crons():
    """Remove all armitage cron entries (session + meta)."""
    existing = _read_crontab()
    lines = [l for l in existing.splitlines()
             if CRON_SESSION_TAG not in l and CRON_META_TAG not in l]
    new_crontab = "\n".join(lines).strip() + "\n" if lines else ""
    _write_crontab(new_crontab)
    logger.info("All armitage crons removed")


def _remove_old_style_cron():
    """Remove the legacy untagged cron entry from cron_setup.py if present."""
    existing = _read_crontab()
    lines = [l for l in existing.splitlines()
             if not (l.strip() and "main.py" in l and "ARMITAGE" not in l)]
    new_crontab = "\n".join(lines).strip() + "\n" if lines else ""
    _write_crontab(new_crontab)


# -------------------------------------------------------------------
# Session execution
# -------------------------------------------------------------------

def _all_sessions_done(schedule):
    return all(s["status"] in ("completed", "failed") for s in schedule["sessions"])


async def run_session(session_id):
    """Execute a specific scraping session."""
    schedule = _load_schedule()
    if not schedule:
        logger.error("No schedule file found. Run 'python scheduler.py generate' first.")
        return

    session = None
    for s in schedule["sessions"]:
        if s["session_id"] == session_id:
            session = s
            break

    if not session:
        logger.error(f"Session {session_id} not found in schedule")
        return

    if session["status"] != "pending":
        logger.warning(f"Session {session_id} is already {session['status']}, skipping")
        return

    # Mark in-progress
    session["status"] = "in_progress"
    session["started_at"] = datetime.now().isoformat(timespec="seconds")
    _save_schedule(schedule)

    companies = [tuple(c) for c in session["companies"]]
    logger.info(f"Starting session {session_id}: {len(companies)} companies on {session['day_name']}")

    try:
        results = await scrape_companies(companies, inter_delay=True)
        session["status"] = "completed"
    except Exception as e:
        logger.exception(f"Session {session_id} failed: {e}")
        session["status"] = "failed"

    session["completed_at"] = datetime.now().isoformat(timespec="seconds")
    _save_schedule(schedule)

    logger.info(f"Session {session_id} finished with status: {session['status']}")

    # Check if all sessions are done -> send digest and clean up crons
    schedule = _load_schedule()
    if _all_sessions_done(schedule) and not schedule.get("digest_sent", False):
        logger.info("All sessions complete. Sending weekly digest and cleaning up...")
        _send_digest_and_cleanup(schedule)


def _send_digest_and_cleanup(schedule):
    """Send the weekly digest email and remove all session crons."""
    recipients_str = os.getenv("EMAIL_RECIPIENTS", "")
    recipients = [r.strip() for r in recipients_str.split(",") if r.strip()]

    if recipients:
        try:
            success = send_digest_report(recipients)
            if success:
                logger.info(f"Digest email sent to {recipients}")
            else:
                logger.warning("Digest email returned False")
        except Exception as e:
            logger.exception(f"Failed to send digest email: {e}")
    else:
        logger.warning("No EMAIL_RECIPIENTS configured, skipping digest email")

    schedule["digest_sent"] = True
    _save_schedule(schedule)

    # Clean up data/input and data/output so next week starts fresh
    for dirname in ("input", "output"):
        d = PROJECT_DIR / "data" / dirname
        if d.exists():
            for f in d.iterdir():
                if f.is_file():
                    f.unlink()
                    logger.info(f"Deleted {f}")
    logger.info("Data directories cleaned")

    # Uninstall all session crons — they're done for the week
    remove_session_crons()
    logger.info("Weekly cycle complete: digest sent, output cleaned, session crons removed")


# -------------------------------------------------------------------
# Weekly generation (meta-cron entrypoint)
# -------------------------------------------------------------------

def generate_and_install():
    """
    Generate a new weekly schedule and install session crons.
    Also handles sending the previous week's digest if it wasn't sent.
    """
    # Handle previous week's leftover digest (only if some sessions actually ran)
    old_schedule = _load_schedule()
    if old_schedule and not old_schedule.get("digest_sent", False):
        completed = [s for s in old_schedule["sessions"] if s["status"] in ("completed", "failed")]
        if completed:
            pending = [s for s in old_schedule["sessions"] if s["status"] == "pending"]
            if pending:
                logger.warning(f"{len(pending)} sessions from previous week never ran")
            logger.info("Sending previous week's digest as safety net...")
            _send_digest_and_cleanup(old_schedule)

    # Remove old-style cron from legacy cron_setup.py
    _remove_old_style_cron()

    # Generate new schedule
    companies = read_companies_from_csv()
    if not companies:
        logger.error("No companies found in CSV. Nothing to schedule.")
        return

    schedule = generate_weekly_schedule(companies)
    _save_schedule(schedule)
    install_session_crons(schedule)

    logger.info(f"New weekly schedule generated: {len(schedule['sessions'])} sessions "
                f"for {len(companies)} companies (week of {schedule['week_of']})")
    print_schedule_status()


# -------------------------------------------------------------------
# Status display
# -------------------------------------------------------------------

def print_schedule_status():
    schedule = _load_schedule()
    if not schedule:
        print("No schedule found. Run 'python scheduler.py generate' to create one.")
        return

    print(f"\nWeek of:     {schedule['week_of']}")
    print(f"Generated:   {schedule['generated_at']}")
    print(f"Companies:   {schedule['total_companies']}")
    print(f"Digest sent: {schedule.get('digest_sent', False)}")
    print(f"\nSessions ({len(schedule['sessions'])}):")

    last_session = None
    for s in schedule["sessions"]:
        companies_str = ", ".join(c[0] for c in s["companies"])
        status_display = s["status"].center(12)
        date_str = s.get("date", "")
        print(f"  [{status_display}] {date_str} {s['day_name']:>9} {s['hour']:02d}:{s['minute']:02d}  "
              f"({len(s['companies'])} companies: {companies_str})")
        last_session = s

    if last_session and not schedule.get("digest_sent", False):
        date_str = last_session.get("date", last_session["day_name"])
        print(f"\nDigest email: after last session on {date_str} "
              f"{last_session['day_name']} ~{last_session['hour']:02d}:{last_session['minute']:02d}")
    elif schedule.get("digest_sent", False):
        print(f"\nDigest email: already sent")
    print()


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

USAGE = """Usage:
    python scheduler.py generate            Generate schedule + install crons
    python scheduler.py run-session <id>    Run a specific session
    python scheduler.py install-meta        Install the weekly meta cron
    python scheduler.py status              Print current schedule status
    python scheduler.py uninstall           Remove all armitage crons
"""

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit(1)

    command = sys.argv[1]

    if command == "generate":
        generate_and_install()
    elif command == "run-session":
        if len(sys.argv) < 3:
            print("Error: session_id required")
            sys.exit(1)
        asyncio.run(run_session(sys.argv[2]))
    elif command == "install-meta":
        install_meta_cron()
    elif command == "status":
        print_schedule_status()
    elif command == "uninstall":
        remove_all_armitage_crons()
        print("All armitage cron entries removed.")
    else:
        print(f"Unknown command: {command}")
        print(USAGE)
        sys.exit(1)
