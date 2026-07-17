TEAM_COLORS = {
    "ARI": {"primary": "#97233F", "secondary": "#000000"},
    "ATL": {"primary": "#A71930", "secondary": "#000000"},
    "BAL": {"primary": "#241773", "secondary": "#000000"},
    "BUF": {"primary": "#00338D", "secondary": "#C60C30"},
    "CAR": {"primary": "#0085CA", "secondary": "#000000"},
    "CHI": {"primary": "#0B162A", "secondary": "#C83803"},
    "CIN": {"primary": "#FB4F14", "secondary": "#000000"},
    "CLE": {"primary": "#FF3C00", "secondary": "#311D00"},
    "DAL": {"primary": "#003594", "secondary": "#869397"},
    "DEN": {"primary": "#FB4F14", "secondary": "#002244"},
    "DET": {"primary": "#0076B6", "secondary": "#B0B7BC"},
    "GB":  {"primary": "#203731", "secondary": "#FFB612"},
    "HOU": {"primary": "#03202F", "secondary": "#A71930"},
    "IND": {"primary": "#002C5F", "secondary": "#A2AAAD"},
    "JAX": {"primary": "#006778", "secondary": "#D7A22A"},
    "KC":  {"primary": "#E31837", "secondary": "#FFB81C"},
    "LA":  {"primary": "#003594", "secondary": "#FFA300"},
    "LAC": {"primary": "#0080C6", "secondary": "#FFC20E"},
    "LV":  {"primary": "#000000", "secondary": "#A5ACAF"},
    "MIA": {"primary": "#008E97", "secondary": "#FC4C02"},
    "MIN": {"primary": "#4F2683", "secondary": "#FFC62F"},
    "NE":  {"primary": "#002244", "secondary": "#C60C30"},
    "NO":  {"primary": "#D3BC8D", "secondary": "#000000"},
    "NYG": {"primary": "#0B2265", "secondary": "#A71930"},
    "NYJ": {"primary": "#125740", "secondary": "#000000"},
    "PHI": {"primary": "#004C54", "secondary": "#A5ACAF"},
    "PIT": {"primary": "#FFB612", "secondary": "#000000"},
    "SEA": {"primary": "#002244", "secondary": "#69BE28"},
    "SF":  {"primary": "#AA0000", "secondary": "#B3995D"},
    "TB":  {"primary": "#D50A0A", "secondary": "#FF7900"},
    "TEN": {"primary": "#0C2340", "secondary": "#4B92DB"},
    "WAS": {"primary": "#5A1414", "secondary": "#FFB612"},
}

# ESPN team abbreviation mapping (nflverse -> ESPN slug for logo URLs)
ESPN_TEAM_MAP = {
    "ARI": "ari", "ATL": "atl", "BAL": "bal", "BUF": "buf",
    "CAR": "car", "CHI": "chi", "CIN": "cin", "CLE": "cle",
    "DAL": "dal", "DEN": "den", "DET": "det", "GB": "gb",
    "HOU": "hou", "IND": "ind", "JAX": "jax", "KC": "kc",
    "LA": "lar",  "LAC": "lac", "LV": "lv",  "MIA": "mia",
    "MIN": "min", "NE": "ne",   "NO": "no",   "NYG": "nyg",
    "NYJ": "nyj", "PHI": "phi", "PIT": "pit", "SEA": "sea",
    "SF": "sf",   "TB": "tb",   "TEN": "ten", "WAS": "was",
}


def get_team_logo_url(team_abbr: str) -> str:
    slug = ESPN_TEAM_MAP.get(team_abbr, team_abbr.lower())
    return f"https://a.espncdn.com/i/teamlogos/nfl/500/{slug}.png"


def get_team_colors(team_abbr: str) -> dict:
    return TEAM_COLORS.get(team_abbr, {"primary": "#013369", "secondary": "#D50A0A"})
