import reflex as rx

# Theme Configuration
THEME_CONFIG = {
    "accent_color": "violet",
    "gray_color": "slate",
    "radius": "large",
    "scaling": "95%",
}

# Custom Colors
COLORS = {
    "bg_dark": "#0f1117",
    "card_bg": "rgba(30, 41, 59, 0.5)",
    "glass_border": "rgba(139, 92, 246, 0.2)",
    "neon_violet": "#8b5cf6",
    "neon_cyan": "#06b6d4",
    "success": "#10b981",
    "warning": "#f59e0b",
}

# Style Mixins
GLASS_STYLE = {
    "background": COLORS["card_bg"],
    "backdrop_filter": "blur(12px)",
    "border": f"1px solid {COLORS['glass_border']}",
    "box_shadow": "0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)",
    "border_radius": "1rem",
}

PAGE_STYLE = {
    "background_color": COLORS["bg_dark"],
    "min_height": "100vh",
    "font_family": "Inter, sans-serif",
}

HEADING_STYLE = {
    "background_image": f"linear-gradient(to right, {COLORS['neon_violet']}, {COLORS['neon_cyan']})",
    "background_clip": "text",
    "-webkit-background-clip": "text",
    "color": "transparent",
    "font_weight": "800",
}

# V3 Specific Themes (Replicating Next.js Design)
VIOLET_STYLE = {
    **GLASS_STYLE,
    "border": "1px solid rgba(139, 92, 246, 0.2)",
    "background": "rgba(139, 92, 246, 0.05)",
}

EMERALD_STYLE = {
    **GLASS_STYLE,
    "border": "1px solid rgba(16, 185, 129, 0.2)",
    "background": "rgba(16, 185, 129, 0.05)",
}

PURPLE_STYLE = {
    **GLASS_STYLE,
    "border": "1px solid rgba(168, 85, 247, 0.2)",
    "background": "rgba(168, 85, 247, 0.05)",
}

RED_STYLE = {
    **GLASS_STYLE,
    "border": "1px solid rgba(239, 68, 68, 0.2)",
    "background": "rgba(239, 68, 68, 0.05)",
}
