import os
import sys
import time
import asyncio
import subprocess

from worker import (
    has_persian_chars,
    match_direct_phrase,
    refine_colloquial_persian,
    parse_delimited_lines,
    translate_all_subtitles_instant,
    generate_ass_file,
    compute_target_resolution,
    format_ass_time
)

async def run_integration_tests():
    print("=== STEP 1: Resolution Preservation & 1080p Quality Checks ===")
    # 1080p landscape (Full HD) must NOT be downscaled!
    w1080_land, h1080_lan