import pandas as pd
import os

from lxml import etree


def parse_ddi_fixed_width(ddi_path: str):
    tree = etree.parse(ddi_path)
    root = tree.getroot()

    colspecs: list[tuple[int, int]] = []
    names: list[str] = []

    for var in root.xpath("//*[local-name()='var']"):
        var_name = var.get("name")
        if not var_name:
            continue

        location = None
        for loc in var.xpath(".//*[local-name()='location']"):
            location = loc
            break
        if location is None:
            continue

        start_pos = (
            location.get("StartPos")
            or location.get("startPos")
            or location.get("startpos")
        )
        width = location.get("width") or location.get("Width")

        if start_pos is None:
            start_node = location.xpath(".//*[local-name()='startPos']/text()")
            if start_node:
                start_pos = start_node[0]

        if width is None:
            width_node = location.xpath(".//*[local-name()='width']/text()")
            if width_node:
                width = width_node[0]

        if start_pos is None or width is None:
            continue

        try:
            start_1 = int(str(start_pos).strip())
            w = int(str(width).strip())
        except Exception:
            continue

        if start_1 <= 0 or w <= 0:
            continue

        start_0 = start_1 - 1
        end_0 = start_0 + w

        colspecs.append((start_0, end_0))
        names.append(var_name)

    return colspecs, names


def convert_fixedwidth_to_csv(txt_path, csv_path, ddi_path):
    if not os.path.exists(txt_path):
        raise FileNotFoundError(f"❌ Fixed-width TXT file not found: {txt_path}")

    if os.path.getsize(txt_path) == 0:
        raise ValueError(f"❌ Fixed-width TXT file is empty: {txt_path}")

    colspecs, names = parse_ddi_fixed_width(ddi_path)
    print(f"✅ Parsed {len(colspecs)} columns from DDI.")

    if not colspecs or not names:
        raise ValueError("❌ No column specifications found in DDI.")

    try:
        df = pd.read_fwf(txt_path, colspecs=colspecs, names=names)
        df.to_csv(csv_path, index=False)
        print(f"✅ Fixed-width TXT converted to CSV: {csv_path}")
    except Exception as e:
        raise RuntimeError(f"❌ Failed to parse fixed-width file: {e}")

    return csv_path
