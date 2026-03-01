"""
Fichiers de style QGIS (.qml) pour les couches SIG.
- analyse_stations_ppp_cote_dor : symbole simple (toutes les analyses).
- hotspots_ppp : symbologie par règles (points chauds, dépassements, etc.).
"""
from __future__ import annotations

from pathlib import Path

# Symbole unique pour la couche « analyse stations » (toutes les analyses)
QML_SIMPLE = '''<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.22.0-Białowieża" styleCategories="Symbology">
  <renderer-v2 type="singleSymbol" enableorderby="0" forceraster="0" symbollevels="0">
    <symbols>
      <symbol type="marker" name="0" alpha="1" force_rhr="0" clip_to_extent="1">
        <data_defined_properties/>
        <layer class="SimpleMarker" enabled="1" locked="0" pass="0">
          <Option type="Map">
            <Option name="angle" type="QString" value="0"/>
            <Option name="cap_style" type="QString" value="square"/>
            <Option name="color" type="QString" value="107,114,128,220"/>
            <Option name="horizontal_anchor_point" type="QString" value="1"/>
            <Option name="joinstyle" type="QString" value="bevel"/>
            <Option name="name" type="QString" value="circle"/>
            <Option name="offset" type="QString" value="0,0"/>
            <Option name="offset_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="offset_unit" type="QString" value="MM"/>
            <Option name="outline_color" type="QString" value="75,85,99,255"/>
            <Option name="outline_style" type="QString" value="solid"/>
            <Option name="outline_width" type="QString" value="0.2"/>
            <Option name="outline_width_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="outline_width_unit" type="QString" value="MM"/>
            <Option name="scale_method" type="QString" value="diameter"/>
            <Option name="size" type="QString" value="1.8"/>
            <Option name="size_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="size_unit" type="QString" value="MM"/>
            <Option name="vertical_anchor_point" type="QString" value="1"/>
          </Option>
          <prop k="angle" v="0"/>
          <prop k="cap_style" v="square"/>
          <prop k="color" v="107,114,128,220"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="joinstyle" v="bevel"/>
          <prop k="name" v="circle"/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="MM"/>
          <prop k="outline_color" v="75,85,99,255"/>
          <prop k="outline_style" v="solid"/>
          <prop k="outline_width" v="0.2"/>
          <prop k="outline_width_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="outline_width_unit" v="MM"/>
          <prop k="scale_method" v="diameter"/>
          <prop k="size" v="1.8"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="MM"/>
          <prop k="vertical_anchor_point" v="1"/>
          <data_defined_properties/>
        </layer>
      </symbol>
    </symbols>
    <rotation/>
    <sizescale/>
  </renderer-v2>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerGeometryType>0</layerGeometryType>
</qgis>
'''

# t = ampleur du dépassement (0 à 1), ratio 1→0, ratio ≥10→1
_HOTSPOT_T = "least(greatest((coalesce(\"ratio_seuil_sanitaire\", 1) - 1)/9, 0), 1)"
# Zone interne : dégradé gris → jaune → orange → rouge selon t (une seule ligne pour éviter newlines)
_HOTSPOT_EXPR_INNER_FILL = (
    "color_rgb("
    "case when " + _HOTSPOT_T + " <= 0.33 then 128 + 122*" + _HOTSPOT_T + "/0.33 "
    "when " + _HOTSPOT_T + " <= 0.66 then 250 + 5*(" + _HOTSPOT_T + "-0.33)/0.33 "
    "else 255 - 35*(" + _HOTSPOT_T + "-0.66)/0.34 end, "
    "case when " + _HOTSPOT_T + " <= 0.33 then 128 + 92*" + _HOTSPOT_T + "/0.33 "
    "when " + _HOTSPOT_T + " <= 0.66 then 220 - 55*(" + _HOTSPOT_T + "-0.33)/0.33 "
    "else 165 - 127*(" + _HOTSPOT_T + "-0.66)/0.34 end, "
    "case when " + _HOTSPOT_T + " <= 0.33 then 128 - 78*" + _HOTSPOT_T + "/0.33 "
    "when " + _HOTSPOT_T + " <= 0.66 then 50 - 50*(" + _HOTSPOT_T + "-0.33)/0.33 "
    "else 38*(" + _HOTSPOT_T + "-0.66)/0.34 end)"
)

# Symbologie hotspots : 12 règles (3 types × 4 classes de taille), symboles à TAILLE FIXE
# type_depassement 1= NQE+sanitaire, 2= NQE seul, 3= sanitaire seul | classe_taille 1–4
#
# Variantes disponibles (HOTSPOT_SYMBOLOGY ci‑dessous) :
#   "default"  — Anneau coloré (vert forêt, bleu, orange) + centre blanc cassé, contour gris fin. Sobre et lisible.
#   "pastel"   — Même principe, couleurs pastel (vert menthe, bleu ciel, pêche). Doux, adapté fond clair.
#   "outline"  — Cercle blanc, contour épais coloré uniquement. Très épuré, type = couleur du trait.
#   "mono"     — Une seule teinte bleue, nuance plus ou moins foncée selon le type. Très sobre.
#
HOTSPOT_SYMBOLOGY = "pastel"  # Changer ici pour "pastel", "outline" ou "mono"
def _expr_for_qml(s: str) -> str:
    """Échappe une expression pour usage dans un attribut XML."""
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("\n", " ")
        .replace("\r", " ")
    )

_HOTSPOT_INNER_EXPR = _expr_for_qml(_HOTSPOT_EXPR_INNER_FILL)

# Tailles fixes par classe (mm) : [ (outer, inner), ... ]
_HOTSPOT_SIZE_CLASSES = [(4, 2.5), (6, 4), (8, 5.5), (10, 7)]

# Palettes par variante : (ring_rgba x3, inner_fill, outline_rgba, outline_width_mm, outline_only)
_HOTSPOT_PALETTES = {
    "default": {
        "rings": ("46,125,50,255", "25,118,210,255", "245,124,0,255"),
        "inner": "252,252,250,255",
        "outline": "55,65,81,255",
        "outline_width": "0.25",
        "outline_only": False,
    },
    "pastel": {
        "rings": ("129,199,132,255", "100,181,246,255", "255,183,77,255"),  # vert menthe, bleu ciel, pêche
        "inner": "255,255,255,255",
        "outline": "120,120,120,255",
        "outline_width": "0.2",
        "outline_only": False,
    },
    "outline": {
        "rings": ("46,125,50,255", "25,118,210,255", "245,124,0,255"),  # contour = couleur type
        "inner": "255,255,255,255",
        "outline": "55,65,81,255",
        "outline_width": "0.6",  # épais pour bien voir la couleur
        "outline_only": True,  # un seul cercle : remplissage blanc, contour coloré
    },
    "mono": {
        "rings": ("66,165,245,255", "33,150,243,255", "2,119,189,255"),  # bleu clair, moyen, foncé
        "inner": "250,250,252,255",
        "outline": "55,65,81,255",
        "outline_width": "0.25",
        "outline_only": False,
    },
}

def _hotspot_palette():
    return _HOTSPOT_PALETTES.get(HOTSPOT_SYMBOLOGY, _HOTSPOT_PALETTES["default"])


def _hotspot_symbol_fixed_xml(symbol_id: int, ring_rgba: str, size_outer: float, size_inner: float) -> str:
    """Symbole 2 couches (ou 1 si outline_only) : anneau coloré + centre clair, ou cercle contour seul. Tailles fixes."""
    pal = _hotspot_palette()
    so, si = str(size_outer), str(size_inner)
    outline_rgba = pal["outline"]
    outline_width = pal["outline_width"]
    inner_fill = pal["inner"]

    if pal["outline_only"]:
        # Un seul cercle : remplissage blanc, contour épais coloré (ring_rgba)
        return f'''      <symbol type="marker" name="{symbol_id}" alpha="1" force_rhr="0" clip_to_extent="1">
        <data_defined_properties/>
        <layer class="SimpleMarker" enabled="1" locked="0" pass="0">
          <Option type="Map">
            <Option name="angle" type="QString" value="0"/>
            <Option name="cap_style" type="QString" value="square"/>
            <Option name="color" type="QString" value="{inner_fill}"/>
            <Option name="horizontal_anchor_point" type="QString" value="1"/>
            <Option name="joinstyle" type="QString" value="bevel"/>
            <Option name="name" type="QString" value="circle"/>
            <Option name="offset" type="QString" value="0,0"/>
            <Option name="offset_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="offset_unit" type="QString" value="MM"/>
            <Option name="outline_color" type="QString" value="{ring_rgba}"/>
            <Option name="outline_style" type="QString" value="solid"/>
            <Option name="outline_width" type="QString" value="{outline_width}"/>
            <Option name="outline_width_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="outline_width_unit" type="QString" value="MM"/>
            <Option name="scale_method" type="QString" value="diameter"/>
            <Option name="size" type="QString" value="{so}"/>
            <Option name="size_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="size_unit" type="QString" value="MM"/>
            <Option name="vertical_anchor_point" type="QString" value="1"/>
          </Option>
          <prop k="angle" v="0"/>
          <prop k="cap_style" v="square"/>
          <prop k="color" v="{inner_fill}"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="joinstyle" v="bevel"/>
          <prop k="name" v="circle"/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="MM"/>
          <prop k="outline_color" v="{ring_rgba}"/>
          <prop k="outline_style" v="solid"/>
          <prop k="outline_width" v="{outline_width}"/>
          <prop k="outline_width_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="outline_width_unit" v="MM"/>
          <prop k="scale_method" v="diameter"/>
          <prop k="size" v="{so}"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="MM"/>
          <prop k="vertical_anchor_point" v="1"/>
          <data_defined_properties/>
        </layer>
      </symbol>'''

    # Deux couches : anneau coloré + centre clair
    return f'''      <symbol type="marker" name="{symbol_id}" alpha="1" force_rhr="0" clip_to_extent="1">
        <data_defined_properties/>
        <layer class="SimpleMarker" enabled="1" locked="0" pass="0">
          <Option type="Map">
            <Option name="angle" type="QString" value="0"/>
            <Option name="cap_style" type="QString" value="square"/>
            <Option name="color" type="QString" value="{ring_rgba}"/>
            <Option name="horizontal_anchor_point" type="QString" value="1"/>
            <Option name="joinstyle" type="QString" value="bevel"/>
            <Option name="name" type="QString" value="circle"/>
            <Option name="offset" type="QString" value="0,0"/>
            <Option name="offset_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="offset_unit" type="QString" value="MM"/>
            <Option name="outline_color" type="QString" value="{outline_rgba}"/>
            <Option name="outline_style" type="QString" value="solid"/>
            <Option name="outline_width" type="QString" value="{outline_width}"/>
            <Option name="outline_width_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="outline_width_unit" type="QString" value="MM"/>
            <Option name="scale_method" type="QString" value="diameter"/>
            <Option name="size" type="QString" value="{so}"/>
            <Option name="size_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="size_unit" type="QString" value="MM"/>
            <Option name="vertical_anchor_point" type="QString" value="1"/>
          </Option>
          <prop k="angle" v="0"/>
          <prop k="cap_style" v="square"/>
          <prop k="color" v="{ring_rgba}"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="joinstyle" v="bevel"/>
          <prop k="name" v="circle"/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="MM"/>
          <prop k="outline_color" v="{outline_rgba}"/>
          <prop k="outline_style" v="solid"/>
          <prop k="outline_width" v="{outline_width}"/>
          <prop k="outline_width_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="outline_width_unit" v="MM"/>
          <prop k="scale_method" v="diameter"/>
          <prop k="size" v="{so}"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="MM"/>
          <prop k="vertical_anchor_point" v="1"/>
          <data_defined_properties/>
        </layer>
        <layer class="SimpleMarker" enabled="1" locked="0" pass="1">
          <Option type="Map">
            <Option name="angle" type="QString" value="0"/>
            <Option name="cap_style" type="QString" value="square"/>
            <Option name="color" type="QString" value="{inner_fill}"/>
            <Option name="horizontal_anchor_point" type="QString" value="1"/>
            <Option name="joinstyle" type="QString" value="bevel"/>
            <Option name="name" type="QString" value="circle"/>
            <Option name="offset" type="QString" value="0,0"/>
            <Option name="offset_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="offset_unit" type="QString" value="MM"/>
            <Option name="outline_color" type="QString" value="{outline_rgba}"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="outline_width" type="QString" value="0"/>
            <Option name="outline_width_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="outline_width_unit" type="QString" value="MM"/>
            <Option name="scale_method" type="QString" value="diameter"/>
            <Option name="size" type="QString" value="{si}"/>
            <Option name="size_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="size_unit" type="QString" value="MM"/>
            <Option name="vertical_anchor_point" type="QString" value="1"/>
          </Option>
          <prop k="angle" v="0"/>
          <prop k="cap_style" v="square"/>
          <prop k="color" v="{inner_fill}"/>
          <prop k="horizontal_anchor_point" v="1"/>
          <prop k="joinstyle" v="bevel"/>
          <prop k="name" v="circle"/>
          <prop k="offset" v="0,0"/>
          <prop k="offset_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="offset_unit" v="MM"/>
          <prop k="outline_color" v="{outline_rgba}"/>
          <prop k="outline_style" v="no"/>
          <prop k="outline_width" v="0"/>
          <prop k="outline_width_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="outline_width_unit" v="MM"/>
          <prop k="scale_method" v="diameter"/>
          <prop k="size" v="{si}"/>
          <prop k="size_map_unit_scale" v="3x:0,0,0,0,0,0"/>
          <prop k="size_unit" v="MM"/>
          <prop k="vertical_anchor_point" v="1"/>
          <data_defined_properties/>
        </layer>
      </symbol>'''


def _hotspot_rules_and_symbols() -> tuple[list[str], list[str]]:
    """Génère 12 règles (type_depassement × classe_taille) et 12 symboles à taille fixe."""
    rules = []
    symbols = []
    labels_type = ("NQE + sanitaire", "NQE", "sanitaire")
    for i in range(12):
        t = i // 4 + 1   # type_depassement 1, 2, 3
        c = i % 4 + 1    # classe_taille 1, 2, 3, 4
        rules.append(
            f'      <rule filter="&quot;type_depassement&quot; = {t} AND &quot;classe_taille&quot; = {c}" '
            f'key="{{r{i}}}" label="{labels_type[t-1]} (taille {c})" symbol="{i}"/>'
        )
        size_outer, size_inner = _HOTSPOT_SIZE_CLASSES[i % 4]
        ring_rgba = _hotspot_palette()["rings"][i // 4]
        symbols.append(_hotspot_symbol_fixed_xml(i, ring_rgba, size_outer, size_inner))
    return rules, symbols


_HOTSPOT_RULES_LINES, _HOTSPOT_SYMBOLS_LINES = _hotspot_rules_and_symbols()

QML_HOTSPOTS = '''<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.22.0-Białowieża" styleCategories="Symbology">
  <renderer-v2 type="RuleRenderer" enableorderby="0" forceraster="0" symbollevels="0">
    <rules key="{a1b2c3d4-0000-0000-0000-000000000001}">
''' + "\n".join(_HOTSPOT_RULES_LINES) + '''
    </rules>
    <symbols>
''' + "\n".join(_HOTSPOT_SYMBOLS_LINES) + '''
    </symbols>
  </renderer-v2>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerGeometryType>0</layerGeometryType>
</qgis>
'''


def write_sig_styles(sig_dir: str | Path) -> list[Path]:
    """
    Écrit les fichiers QML : symbole simple pour analyse_stations, règles pour hotspots.
    QGIS charge le style si le .qml a le même nom que le .geojson.
    """
    sig_dir = Path(sig_dir)
    sig_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    (sig_dir / "analyse_stations_ppp_cote_dor.qml").write_text(QML_SIMPLE, encoding="utf-8")
    written.append(sig_dir / "analyse_stations_ppp_cote_dor.qml")
    (sig_dir / "hotspots_ppp.qml").write_text(QML_HOTSPOTS, encoding="utf-8")
    written.append(sig_dir / "hotspots_ppp.qml")
    return written
