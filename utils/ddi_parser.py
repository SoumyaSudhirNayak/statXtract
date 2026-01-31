from lxml import etree
from typing import List, Optional, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DDIVariable:
    def __init__(self):
        self.name: str = ""
        self.label: str = ""
        self.start_pos: Optional[int] = None
        self.width: Optional[int] = None
        self.decimals: int = 0
        self.data_type: str = "string"  # numeric, string
        self.categories: List[Dict[str, Any]] = []  # List of {code, label, frequency}
        self.missing_values: List[str] = []
        self.universe: str = ""
        self.concept: str = ""
        self.question: str = ""

def _get_text(element, xpath, namespaces=None):
    """Helper to safely get text from an element."""
    nodes = element.xpath(xpath, namespaces=namespaces)
    if nodes:
        return nodes[0].text if nodes[0].text else ""
    return ""

def parse_ddi_xml(ddi_path: str) -> Dict[str, Any]:
    """
    Parses a DDI XML file and returns authoritative metadata.
    """
    try:
        tree = etree.parse(ddi_path)
        root = tree.getroot()
    except Exception as e:
        logger.error(f"Failed to parse DDI XML {ddi_path}: {e}")
        raise

    # Handle namespaces by using local-name() in XPath or ignoring them
    # For robustness, we'll use local-name() approach mostly
    
    # Extract Study Title
    title = ""
    # Try multiple paths for title
    title_nodes = root.xpath("//*[local-name()='stdyDscr']//*[local-name()='titl']")
    if title_nodes and title_nodes[0].text:
        title = title_nodes[0].text.strip()
    
    variables = []
    
    # Find all 'var' elements under 'dataDscr'
    var_elements = root.xpath("//*[local-name()='dataDscr']//*[local-name()='var']")
    
    for var_elem in var_elements:
        var = DDIVariable()
        var.name = var_elem.get("name") or var_elem.get("ID")
        
        # Label
        labl_nodes = var_elem.xpath("./*[local-name()='labl']")
        if labl_nodes and labl_nodes[0].text:
            var.label = labl_nodes[0].text.strip()
            
        # Location (StartPos, width)
        loc_nodes = var_elem.xpath("./*[local-name()='location']")
        if loc_nodes:
            loc = loc_nodes[0]
            # Try attributes first (case insensitive)
            start_pos = loc.get("StartPos") or loc.get("startPos")
            width = loc.get("width") or loc.get("Width")
            
            if start_pos:
                try:
                    var.start_pos = int(start_pos)
                except ValueError:
                    pass
            
            if width:
                try:
                    var.width = int(width)
                except ValueError:
                    pass
        
        # Format / Decimals
        # varFormat type="numeric" schema="other"
        fmt_nodes = var_elem.xpath("./*[local-name()='varFormat']")
        if fmt_nodes:
            fmt = fmt_nodes[0]
            var_type = fmt.get("type")
            if var_type:
                var.data_type = var_type.lower() # numeric, character
            
            dcml = fmt.get("dcml")
            if dcml:
                try:
                    var.decimals = int(dcml)
                except ValueError:
                    pass

        # Categories
        # <catgry><catValu>1</catValu><labl>Yes</labl><catStat type="freq">123</catStat></catgry>
        cat_nodes = var_elem.xpath("./*[local-name()='catgry']")
        for cat in cat_nodes:
            val_nodes = cat.xpath("./*[local-name()='catValu']")
            lab_nodes = cat.xpath("./*[local-name()='labl']")
            stat_nodes = cat.xpath("./*[local-name()='catStat'][@type='freq']")
            
            if val_nodes and val_nodes[0].text:
                code = val_nodes[0].text.strip()
                label = lab_nodes[0].text.strip() if (lab_nodes and lab_nodes[0].text) else code
                freq = None
                if stat_nodes and stat_nodes[0].text:
                    try:
                        freq = int(stat_nodes[0].text.strip())
                    except ValueError:
                        pass
                
                var.categories.append({
                    "code": code,
                    "label": label,
                    "frequency": freq
                })

        # Missing Values
        # <invalrng><item VALUE="99"/></invalrng>
        inval_nodes = var_elem.xpath("./*[local-name()='invalrng']//*[local-name()='item']")
        for item in inval_nodes:
            val = item.get("VALUE") or item.get("value")
            if val:
                var.missing_values.append(val)
                
        # Universe
        univ_nodes = var_elem.xpath("./*[local-name()='universe']")
        if univ_nodes and univ_nodes[0].text:
            var.universe = univ_nodes[0].text.strip()
            
        # Question
        qstn_nodes = var_elem.xpath("./*[local-name()='qstn']//*[local-name()='qstnLit']")
        if qstn_nodes and qstn_nodes[0].text:
            var.question = qstn_nodes[0].text.strip()

        # Concept
        concept_nodes = var_elem.xpath("./*[local-name()='concept']")
        if concept_nodes and concept_nodes[0].text:
             var.concept = concept_nodes[0].text.strip()

        variables.append(var)

    return {
        "title": title,
        "variables": variables
    }

if __name__ == "__main__":
    # Test
    pass
