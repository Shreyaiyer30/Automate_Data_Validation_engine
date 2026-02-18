def score_ring(score: float, label: str = "Quality Score") -> str:
    """Returns HTML for a visual circular quality score ring."""
    pct  = max(0.0, min(100.0, score))
    r    = 48
    circ = 2 * 3.14159 * r
    dash = (pct / 100) * circ
    if pct >= 90:   color = "#22c55e"
    elif pct >= 70: color = "#f59e0b"
    else:           color = "#ef4444"

    return f"""
    <div class="score-ring-wrap">
        <div class="score-ring">
            <svg width="120" height="120" viewBox="0 0 120 120">
                <circle cx="60" cy="60" r="{r}"
                    fill="none" stroke="var(--border)" stroke-width="8"/>
                <circle cx="60" cy="60" r="{r}"
                    fill="none" stroke="{color}" stroke-width="8"
                    stroke-linecap="round"
                    stroke-dasharray="{dash:.1f} {circ:.1f}"/>
            </svg>
            <div class="score-ring-label">
                <div class="score-number" style="color:{color}">{pct:.0f}</div>
                <div class="score-sub">/100</div>
            </div>
        </div>
        <div style="font-size:0.78rem; color:var(--muted); font-weight:500;">{label}</div>
    </div>
    """
