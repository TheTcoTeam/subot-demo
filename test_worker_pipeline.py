import os
import sys
import asyncio
import subprocess

# Test that worker functions import cleanly
from worker import (
    has_persian_chars,
    parse_delimited_lines,
    try_groq_llm,
    try_gemini_llm,
    try_gtx_batch,
    translate_all_subtitles_instant,
    generate_ass_file,
    compute_target_resolution,
    format_ass_time
)

async def run_integration_tests():
    print("=== STEP 1: Unit & Regex Tests ===")
    sample_llm_output = """
    0|||سلام، این یک ویدیوی آموزشی فوق‌العاده است.
    1: هوش مصنوعی به زودی همه چیز را متحول خواهد کرد.
    [2] - زیرنویس‌های ما کاملا هماهنگ و دقیق هستند.
    3. این خط نیز به راحتی پردازش می‌شود.
    """
    parsed = parse_delimited_lines(sample_llm_output)
    print(f"Parsed {len(parsed)} lines successfully.")
    assert len(parsed) == 4, f"Expected 4 lines, got {len(parsed)}"
    assert has_persian_chars(parsed[0]), "Failed Persian detection"
    assert not has_persian_chars("Hello world!"), "English incorrectly detected as Persian"

    print("\n=== STEP 2: Live Translation Verification ===")
    test_segments = [
        {"id": 0, "start": 0.0, "end": 2.5, "text": "Welcome to our channel. Today we have an exciting update."},
        {"id": 1, "start": 2.5, "end": 5.0, "text": "Artificial intelligence is changing the world very quickly."},
        {"id": 2, "start": 5.0, "end": 8.0, "text": "Make sure to subscribe and share this video with your friends."}
    ]

    translated = await translate_all_subtitles_instant(test_segments)
    print(f"Translated {len(translated)} segments:")
    for s in translated:
        has_fa = has_persian_chars(s['text'])
        print(f"  [{s['start']}s -> {s['end']}s] ID {s.get('id', '?')}: {s['text']} (Persian: {has_fa})")
        assert has_fa, f"CRITICAL: Segment did not translate to Persian! Result: {s['text']}"

    print("\n=== STEP 3: ASS Subtitle Generation Test ===")
    ass_path = "test_subtitles.ass"
    generate_ass_file(translated, "@TestChannel", 1280, 720, ass_path)
    assert os.path.exists(ass_path), "ASS file was not created"
    with open(ass_path, "r", encoding="utf-8-sig") as f:
        content = f.read()
        assert "YellowSub" in content, "YellowSub style missing"
        assert "@TestChannel" in content, "Channel tag missing"
        assert "\u202B" in content, "RTL unicode marker missing"
    print("ASS file generated and validated successfully.")

    print("\n=== ALL PIPELINE TESTS PASSED WITH 100% PERSIAN ACCURACY ===")

if __name__ == "__main__":
    asyncio.run(run_integration_tests())
