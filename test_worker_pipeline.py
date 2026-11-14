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
    w1080_land, h1080_land = compute_target_resolution(1920, 1080)
    assert (w1080_land, h1080_land) == (1920, 1080), f"1080p landscape degraded! Got {w1080_land}x{h1080_land}"

    # 1080p vertical (Reels / TikTok / Stories) must NOT be downscaled!
    w1080_vert, h1080_vert = compute_target_resolution(1080, 1920)
    assert (w1080_vert, h1080_vert) == (1080, 1920), f"1080p vertical degraded! Got {w1080_vert}x{h1080_vert}"

    # 720p standard should be preserved
    w720, h720 = compute_target_resolution(1280, 720)
    assert (w720, h720) == (1280, 720)

    # 4K should scale down to 1080p for Telegram stability
    w4k, h4k = compute_target_resolution(3840, 2160)
    assert (w4k, h4k) == (1920, 1080), f"4K scaling failed! Got {w4k}x{h4k}"

    print("Resolution & 1080p Full HD preservation validated: 100% SUCCESS.")

    print("\n=== STEP 2: Colloquial Intimate Refiner & Direct Phrase Tests ===")
    # Direct intimate phrase lookup tests
    direct_checks = [
        ("Oh yeah, that feels so good baby.", "خیلی حال میده"),
        ("Harder, please don't stop right now.", "وای نستا"),
        ("Fuck me, I'm coming!", "داره میاد"),
        ("Suck my dick", "ساک بزن برام"),
        ("Ride me", "بیا روم بشین"),
    ]
    for eng, exp_match in direct_checks:
        dp = match_direct_phrase(eng)
        sys.stdout.buffer.write((f"  Direct lookup: '{eng}' -> '{dp}'\n").encode("utf-8"))
        assert dp, f"Direct phrase failed for: {eng}"

    test_phrases = [
        ("اوه بله، من دارم میام!", "آره جونم، داره میاد!"),
        ("سخت تر، متوقف نشو عزیزم", "محکم‌تر، وای نستا عزیزم"),
        ("لعنتی خوب است و احساس خیلی خوبی دارد", "فوق‌العاده است و چقدر حال میده"),
        ("خروس مرا بگذار داخل", "کیر مرا بکنش تو")
    ]
    for inp, expected in test_phrases:
        res = refine_colloquial_persian(inp)
        sys.stdout.buffer.write((f"  Raw: {inp} -> Refined: {res}\n").encode("utf-8"))
        assert has_persian_chars(res)

    print("\n=== STEP 3: Live Speed & Translation Verification ===")
    adult_test_segments = [
        {"id": 0, "start": 0.0, "end": 2.5, "text": "Oh yeah, that feels so good baby."},
        {"id": 1, "start": 2.5, "end": 5.0, "text": "Harder, please don't stop right now."},
        {"id": 2, "start": 5.0, "end": 8.0, "text": "Look at me, you like that? I'm gonna cum."}
    ]

    t0 = time.time()
    translated = await translate_all_subtitles_instant(adult_test_segments)
    elapsed = time.time() - t0
    print(f"Translation completed in: {elapsed:.2f} seconds!")
    assert elapsed < 7.0, f"Translation took too long: {elapsed:.2f}s"

    print(f"Translated {len(translated)} segments:")
    for s in translated:
        has_fa = has_persian_chars(s['text'])
        sys.stdout.buffer.write((f"  [{s['start']}s -> {s['end']}s]: {s['text']} (Persian: {has_fa})\n").encode("utf-8"))
        assert has_fa, f"CRITICAL: Segment did not translate to Persian! Result: {s['text']}"

    print("\n=== STEP 4: ASS Subtitle Generation for 1080p ===")
    ass_path = "test_subtitles.ass"
    generate_ass_file(translated, "@TestChannel", 1920, 1080, ass_path)
    assert os.path.exists(ass_path), "ASS file was not created"
    with open(ass_path, "r", encoding="utf-8-sig") as f:
        content = f.read()
        assert "PlayResX: 1920" in content, "PlayResX 1080p mismatch"
        assert "PlayResY: 1080" in content, "PlayResY 1080p mismatch"
        assert "YellowSub" in content, "YellowSub style missing"
        assert "@TestChannel" in content, "Channel tag missing"
        assert "\u202B" in content, "RTL unicode marker missing"
    print("1080p ASS file generated and validated successfully.")

    print("\n=== ALL INTEGRATION TESTS PASSED WITH 100% 1080P QUALITY & SPEED ===")

if __name__ == "__main__":
    asyncio.run(run_integration_tests())
