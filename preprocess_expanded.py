"""
Expanded Preprocessor for Indian Job Market × AI Exposure Visualization
=========================================================================
Combines:
  1. Naukri.com CSV data (97K listings) — granular organized-sector data
  2. PLFS 2023-24 official estimates — full Indian workforce (≈607M workers)
     including the massive informal/unorganized sector (93% of workforce)

Data sources for workforce estimates:
  - PLFS Annual Report July 2023 – June 2024 (MoSPI / NSO)
  - Wikipedia "Economy of India" article citing PLFS, NITI Aayog, ILO, World Bank
  - PIB press releases, Economic Survey 2024-25, IBEF sector reports
"""
import csv
import re
import json
from collections import defaultdict
import statistics
import math

# ═══════════════════════════════════════════════════════════════════════
#  PLFS 2023-24: Full Indian Workforce Occupation Categories
#  Total workforce: ~607 million (PLFS Annual Report 2023-24)
#  Organized sector: ~7% (~42M)  |  Unorganized: ~93% (~565M)
#
#  The Naukri.com data captures only organized-sector job listings.
#  Below we add the MISSING workforce segments from PLFS estimates.
# ═══════════════════════════════════════════════════════════════════════

# Employment distribution from PLFS 2023-24 and official sources:
#   Agriculture: 46.1% of 607M = ~280M
#   Manufacturing: 11.4% = ~69M
#   Construction: ~71M (PLFS/Wikipedia citation)
#   Trade/Hotels/Restaurants: ~12% = ~73M
#   Transport/Storage/Communications: ~5.7% = ~35M
#   Mining & Quarrying: ~0.4% = ~2.4M
#   Financial/Real Estate/Professional Services: ~3.4% = ~21M
#   Public Admin/Education/Health/Other Services: ~12% = ~73M
#   Electricity/Gas/Water: ~0.6% = ~3.6M

# PLFS-sourced categories that ARE NOT captured by Naukri.com
# (the informal sector that dwarfs the organized sector)
PLFS_CATEGORIES = {
    "Farmers & Agricultural Workers": {
        "workers_millions": 280,
        "ai_exposure": 1,
        "avg_salary_lakhs": 0.9,  # ≈₹7,500/month (PLFS rural agri wages)
        "avg_experience": 15,
        "source": "PLFS 2023-24: 46.1% of 607M workforce",
        "description": "Cultivators, agricultural labourers, farmers, dairy workers, fishermen, forestry workers. India's largest employment sector — mostly self-employed smallholders on <1 hectare. Minimal AI exposure: physical outdoor labour, rain-dependent, subsistence-level tasks.",
        "top_cities": [["Rural India", 240000000], ["Uttar Pradesh", 45000000], ["Bihar", 22000000], ["Madhya Pradesh", 18000000], ["Rajasthan", 16000000]],
        "sector": "agriculture",
    },
    "Construction Labourers": {
        "workers_millions": 62,
        "ai_exposure": 1,
        "avg_salary_lakhs": 1.8,  # ≈₹15,000/month for unskilled
        "avg_experience": 8,
        "source": "PLFS 2023-24 + Knight Frank/World Bank (71M total, minus 9M organized)",
        "description": "Masons, brick-layers, bar-benders, painters, plasterers, road construction workers, MGNREGA workers. 83.2M people worked under MGNREGA in FY24 alone. Heavily physical, on-site labour with near-zero AI exposure.",
        "top_cities": [["Rural India", 25000000], ["Delhi NCR", 5000000], ["Mumbai", 4000000], ["Bengaluru", 3000000], ["Hyderabad", 2500000]],
        "sector": "construction",
    },
    "Street Vendors & Hawkers": {
        "workers_millions": 20,
        "ai_exposure": 1,
        "avg_salary_lakhs": 1.2,
        "avg_experience": 10,
        "source": "Ministry of Housing 2024 estimates; Street Vendors Act 2014",
        "description": "Vegetable sellers, fruit vendors, chai-wallahs, flower sellers, roadside food stalls. An estimated 10-20M urban street vendors across India. Personal, location-dependent, cash-based economic activity.",
        "top_cities": [["Mumbai", 2500000], ["Delhi", 2000000], ["Kolkata", 1500000], ["Chennai", 1200000], ["Bengaluru", 800000]],
        "sector": "trade",
    },
    "Domestic Workers & Helpers": {
        "workers_millions": 15,
        "ai_exposure": 1,
        "avg_salary_lakhs": 0.8,
        "avg_experience": 7,
        "source": "ILO India domestic workers estimate; National Domestic Workers Movement",
        "description": "Maids, cooks, nannies, guards/watchmen, home-based care workers. Overwhelmingly informal, predominantly women, mostly in urban households. Personal service tasks with near-zero AI automation potential.",
        "top_cities": [["Delhi NCR", 2000000], ["Mumbai", 1800000], ["Bengaluru", 1200000], ["Hyderabad", 1000000], ["Chennai", 900000]],
        "sector": "services",
    },
    "Textile & Garment Workers": {
        "workers_millions": 30,
        "ai_exposure": 2,
        "avg_salary_lakhs": 1.5,
        "avg_experience": 6,
        "source": "Ministry of Textiles (35M+ direct employment); IBEF",
        "description": "Weavers, tailors, garment factory workers, handloom artisans, dyeing workers, embroidery workers. India's second-largest employment sector after agriculture. Low AI exposure: manual dexterity, craft-based work.",
        "top_cities": [["Tirupur", 3000000], ["Surat", 2500000], ["Ludhiana", 2000000], ["Mumbai", 1800000], ["Bengaluru", 1500000]],
        "sector": "manufacturing",
    },
    "Auto & Rickshaw Drivers": {
        "workers_millions": 18,
        "ai_exposure": 3,
        "avg_salary_lakhs": 2.4,
        "avg_experience": 8,
        "source": "Transport sector estimates; Ola/Uber India driver base data",
        "description": "Auto-rickshaw drivers, taxi/cab drivers (incl. app-based), truck drivers, bus drivers. Navigation & route planning increasingly AI-assisted, but driving itself remains manual in India's complex traffic. Autonomous vehicles decades away for Indian roads.",
        "top_cities": [["Delhi NCR", 2500000], ["Mumbai", 2000000], ["Bengaluru", 1500000], ["Chennai", 1200000], ["Hyderabad", 1000000]],
        "sector": "transport",
    },
    "Retail Shop Workers": {
        "workers_millions": 30,
        "ai_exposure": 3,
        "avg_salary_lakhs": 1.8,
        "avg_experience": 5,
        "source": "PLFS; 35M retail employment (IBEF); kirana store employment estimates",
        "description": "Kirana/grocery store workers, small retail shop assistants, market stall operators, wholesale traders, mandi workers. India has 12-14M retail outlets, mostly unorganized. Limited AI exposure: personal interaction, cash handling, inventory by experience.",
        "top_cities": [["Delhi NCR", 3000000], ["Mumbai", 2800000], ["Kolkata", 2000000], ["Chennai", 1500000], ["Bengaluru", 1400000]],
        "sector": "trade",
    },
    "Mining & Quarry Workers": {
        "workers_millions": 6,
        "ai_exposure": 1,
        "avg_salary_lakhs": 2.0,
        "avg_experience": 10,
        "source": "Ministry of Mines; 11M direct+indirect (Wikipedia/Economy of India)",
        "description": "Coal miners, stone quarry workers, sand miners, iron ore workers, limestone miners. Heavily physical, hazardous work with minimal technology adoption in the unorganized segment. Large-scale mines use some automation but the workforce is predominantly manual.",
        "top_cities": [["Jharkhand", 1500000], ["Odisha", 1000000], ["Chhattisgarh", 800000], ["Rajasthan", 700000], ["Madhya Pradesh", 500000]],
        "sector": "mining",
    },
    "Artisans & Handicraft Workers": {
        "workers_millions": 12,
        "ai_exposure": 1,
        "avg_salary_lakhs": 1.0,
        "avg_experience": 12,
        "source": "Ministry of Textiles; Handicraft Export Council; khadi sector estimates",
        "description": "Potters, carpenters, blacksmiths, leather workers, bamboo craft workers, jewelry artisans, stone carvers, khadi weavers. Traditional craft-based occupations passed through generations. AI has essentially zero capability to replicate these physical-creative skills.",
        "top_cities": [["Jaipur", 1500000], ["Moradabad", 800000], ["Varanasi", 700000], ["Lucknow", 600000], ["Firozabad", 500000]],
        "sector": "manufacturing",
    },
    "Government & Defense Personnel": {
        "workers_millions": 20,
        "ai_exposure": 4,
        "avg_salary_lakhs": 5.0,
        "avg_experience": 12,
        "source": "6th & 7th CPC data; Armed Forces + Central + State employees",
        "description": "Central & state government clerks, peons, officers; Armed Forces (1.4M active); paramilitary (1M+); police (2.5M). Government processes are rules-based and paper-heavy — many clerical tasks are AI-automatable, but security/policing/defense roles have low exposure.",
        "top_cities": [["Delhi NCR", 3000000], ["State Capitals", 5000000], ["Cantonment Areas", 2000000], ["Lucknow", 1000000], ["Chandigarh", 500000]],
        "sector": "government",
    },
    "Salon & Beauty Workers": {
        "workers_millions": 6,
        "ai_exposure": 1,
        "avg_salary_lakhs": 1.8,
        "avg_experience": 5,
        "source": "NSSO estimates; beauty & wellness industry reports",
        "description": "Barbers, hairdressers, beauticians, spa workers, laundry/dhobi workers. Highly personal, physical-touch services. India has millions of neighbourhood barbershops and beauty parlours. AI cannot perform these manual services.",
        "top_cities": [["Mumbai", 600000], ["Delhi", 550000], ["Bengaluru", 400000], ["Hyderabad", 350000], ["Chennai", 300000]],
        "sector": "services",
    },
    "Food Processing & Beverage Workers": {
        "workers_millions": 8,
        "ai_exposure": 2,
        "avg_salary_lakhs": 1.5,
        "avg_experience": 5,
        "source": "Ministry of Food Processing Industries; NSSO estimates",
        "description": "Workers in food processing units, bakeries, beverage plants, cold storage, dairy processing, rice/flour mills, sugar mills. Mix of factory work and small-scale units. Some automation in large plants, but vast majority is manual labour.",
        "top_cities": [["Gujarat", 1200000], ["Maharashtra", 1000000], ["Tamil Nadu", 800000], ["Uttar Pradesh", 700000], ["Andhra Pradesh", 600000]],
        "sector": "manufacturing",
    },
}

# ── Naukri.com organized sector categories (same as original) ──
NAUKRI_CATEGORIES = {
    "Software Developers": {
        "keywords": ["software developer", "software engineer", "full stack developer",
                      "fullstack developer", "full stack engineer", "frontend developer",
                      "backend developer", "web developer", "react developer",
                      "angular developer", "node developer", "application developer",
                      "application lead", "developer - l3", "software development engineer",
                      "software development lead", "senior software engineer",
                      "dot net developer", "php developer", "ruby developer",
                      "golang developer", "rust developer", "mobile developer",
                      "android developer", "ios developer", "flutter developer",
                      "mern stack", "mean stack", "application designer"],
        "skill_keywords": ["react", "angular", "node", "javascript", "typescript",
                           "html", "css", "vue", "django", "flask", "spring",
                           "asp.net", ".net", "swift", "kotlin", "flutter"],
        "ai_exposure": 8,
    },
    "Data Scientists & ML Engineers": {
        "keywords": ["data scientist", "machine learning", "ml engineer", "ai engineer",
                     "ai / ml engineer", "deep learning", "nlp engineer",
                     "computer vision", "data science", "research scientist",
                     "artificial intelligence"],
        "skill_keywords": ["tensorflow", "pytorch", "machine learning", "deep learning",
                           "nlp", "computer vision", "keras"],
        "ai_exposure": 7,
    },
    "Data Engineers & Analysts": {
        "keywords": ["data engineer", "data analyst", "senior data engineer",
                     "business intelligence", "bi developer", "bi analyst",
                     "etl developer", "analytics", "mis executive",
                     "financial analyst", "data analytics"],
        "skill_keywords": ["spark", "hadoop", "airflow", "kafka", "snowflake",
                           "tableau", "power bi", "etl"],
        "ai_exposure": 7,
    },
    "DevOps & Cloud Engineers": {
        "keywords": ["devops", "cloud engineer", "site reliability", "sre",
                     "infrastructure engineer", "platform engineer",
                     "cloud architect", "aws engineer", "azure engineer",
                     "kubernetes", "docker engineer"],
        "skill_keywords": ["kubernetes", "docker", "terraform", "ansible",
                           "jenkins", "aws", "azure", "gcp", "ci/cd"],
        "ai_exposure": 6,
    },
    "Cybersecurity & Networking": {
        "keywords": ["security architect", "security engineer", "security analyst",
                     "cybersecurity", "information security", "network engineer",
                     "network administrator", "penetration tester",
                     "soc analyst", "security consultant"],
        "skill_keywords": ["firewall", "siem", "ids", "ips", "penetration testing",
                           "vulnerability", "cisco", "ccna"],
        "ai_exposure": 5,
    },
    "IT Support & System Admin": {
        "keywords": ["application support", "technical support", "it support",
                     "system administrator", "desktop support", "help desk",
                     "application tech support", "lead administrator",
                     "infrastructure support", "it administrator"],
        "skill_keywords": ["active directory", "windows server", "linux admin",
                           "troubleshooting", "ticketing"],
        "ai_exposure": 6,
    },
    "SAP & ERP Consultants": {
        "keywords": ["sap consultant", "sap certified", "erp", "sap fico",
                     "sap abap", "sap mm", "sap sd", "sap hana",
                     "oracle apps", "oracle erp", "sap basis"],
        "skill_keywords": ["sap", "erp", "oracle apps", "dynamics 365"],
        "ai_exposure": 6,
    },
    "Software Testing & QA": {
        "keywords": ["quality engineer", "test engineer", "qa engineer",
                     "software testing", "quality analyst", "test lead",
                     "automation tester", "manual tester", "sdet",
                     "performance tester", "quality engineer (tester)"],
        "skill_keywords": ["selenium", "jmeter", "appium", "cypress",
                           "test automation", "software testing"],
        "ai_exposure": 8,
    },
    "Project & Product Managers": {
        "keywords": ["project manager", "product manager", "program manager",
                     "scrum master", "agile coach", "delivery manager",
                     "technical project manager", "business architect",
                     "solution architect", "engineering manager",
                     "senior group product manager"],
        "skill_keywords": ["agile", "scrum", "jira", "confluence",
                           "project management", "product management"],
        "ai_exposure": 5,
    },
    "Business Analysts": {
        "keywords": ["business analyst", "management analyst",
                     "systems analyst", "requirements analyst",
                     "functional analyst", "process analyst"],
        "skill_keywords": ["business analysis", "requirements gathering",
                           "brd", "use cases"],
        "ai_exposure": 7,
    },
    "Sales Executives & Managers": {
        "keywords": ["sales executive", "sales manager", "area sales manager",
                     "sales officer", "field sales", "inside sales",
                     "sales coordinator", "sales engineer", "international sales",
                     "tele sales", "sales head", "key account manager",
                     "relationship manager - banca"],
        "skill_keywords": ["sales", "business development", "crm", "lead generation"],
        "ai_exposure": 4,
    },
    "Business Development": {
        "keywords": ["business development executive", "business development manager",
                     "business development associate", "bde", "bdm",
                     "business development"],
        "skill_keywords": [],
        "ai_exposure": 4,
    },
    "Marketing & Digital Marketing": {
        "keywords": ["marketing executive", "digital marketing", "marketing manager",
                     "seo", "sem", "social media", "content marketing",
                     "performance marketing", "brand manager",
                     "market research", "nfl content writer",
                     "sports content writer", "american sports writer"],
        "skill_keywords": ["seo", "sem", "google ads", "facebook ads",
                           "social media", "content marketing"],
        "ai_exposure": 7,
    },
    "Graphic Design & UI/UX": {
        "keywords": ["graphic designer", "ui designer", "ux designer",
                     "ui/ux", "visual designer", "creative designer",
                     "design engineer", "product designer", "web designer",
                     "motion graphics"],
        "skill_keywords": ["figma", "photoshop", "illustrator", "adobe xd",
                           "sketch", "invision"],
        "ai_exposure": 7,
    },
    "Content Writers & Editors": {
        "keywords": ["content writer", "copywriter", "technical writer",
                     "editor", "content creator", "content strategist",
                     "blog writer", "scriptwriter"],
        "skill_keywords": ["content writing", "copywriting", "blogging",
                           "creative writing"],
        "ai_exposure": 9,
    },
    "HR & Talent Acquisition": {
        "keywords": ["hr", "human resource", "talent acquisition", "recruiter",
                     "hr recruiter", "hr executive", "hr manager",
                     "hr generalist", "us it lead recruiter",
                     "talent sourcing", "hr business partner"],
        "skill_keywords": ["recruitment", "hiring", "manpower",
                           "staffing", "talent acquisition"],
        "ai_exposure": 6,
    },
    "Accountants & Finance": {
        "keywords": ["accountant", "senior accountant", "accounts executive",
                     "financial analyst", "finance manager", "finance executive",
                     "bookkeeping", "financial controller", "chartered accountant",
                     "tax consultant", "audit", "account executive",
                     "record to report"],
        "skill_keywords": ["accounting", "tally", "gst", "tds",
                           "income tax", "balance sheet"],
        "ai_exposure": 8,
    },
    "Customer Support & BPO": {
        "keywords": ["customer support", "customer service", "customer care",
                     "call center", "bpo", "voice process", "chat process",
                     "non voice", "telecaller", "tele caller",
                     "international voice", "customer service executive",
                     "customer support executive", "customer care executive"],
        "skill_keywords": ["customer service", "bpo", "call center",
                           "voice process", "chat support"],
        "ai_exposure": 9,
    },
    "Operations & Logistics": {
        "keywords": ["operations manager", "operations executive",
                     "logistics", "supply chain", "warehouse",
                     "procurement", "purchase executive", "inventory",
                     "dispatch", "store manager", "operations"],
        "skill_keywords": ["supply chain", "logistics", "procurement",
                           "inventory management"],
        "ai_exposure": 4,
    },
    "Teaching & Training": {
        "keywords": ["teacher", "trainer", "professor", "lecturer",
                     "faculty", "tutor", "academic", "instructor",
                     "training", "education", "teaching"],
        "skill_keywords": ["teaching", "training", "education", "curriculum"],
        "ai_exposure": 4,
    },
    "Healthcare & Medical": {
        "keywords": ["doctor", "physician", "surgeon", "nurse", "nursing",
                     "medical officer", "cardiologist", "pharmacist",
                     "pharmacy", "physiotherapist", "dentist",
                     "medical billing", "clinical", "healthcare",
                     "medical assistant", "lab technician"],
        "skill_keywords": ["medical", "clinical", "healthcare",
                           "patient care", "pharmacy"],
        "ai_exposure": 3,
    },
    "Mechanical & Manufacturing": {
        "keywords": ["mechanical engineer", "production engineer",
                     "manufacturing", "maintenance engineer",
                     "maintenance technician", "cnc", "design engineer",
                     "quality engineer", "process engineer",
                     "tool designer", "plant engineer", "assistant manager- bakery"],
        "skill_keywords": ["autocad", "solidworks", "mechanical design",
                           "cnc", "manufacturing"],
        "ai_exposure": 3,
    },
    "Civil & Construction": {
        "keywords": ["civil engineer", "site engineer", "construction",
                     "structural engineer", "billing engineer",
                     "quantity surveyor", "architect", "planning engineer",
                     "estimation engineer", "pole deployment"],
        "skill_keywords": ["autocad", "revit", "civil engineering",
                           "structural", "construction"],
        "ai_exposure": 3,
    },
    "Electrical & Electronics": {
        "keywords": ["electrical engineer", "electronics engineer",
                     "embedded", "vlsi", "hardware engineer",
                     "instrumentation", "automation engineer",
                     "plc", "scada", "electrical"],
        "skill_keywords": ["embedded", "vlsi", "plc", "scada", "circuit design"],
        "ai_exposure": 3,
    },
    "Legal & Compliance": {
        "keywords": ["legal", "lawyer", "advocate", "compliance",
                     "company secretary", "legal counsel", "paralegal",
                     "regulatory", "law", "litigation"],
        "skill_keywords": ["legal", "compliance", "regulatory", "litigation"],
        "ai_exposure": 6,
    },
    "Banking & Insurance": {
        "keywords": ["branch manager", "relationship manager",
                     "branch relationship officer", "loan officer",
                     "sales officer home loan", "insurance",
                     "underwriter", "credit analyst", "risk analyst",
                     "fraud prevention", "banking"],
        "skill_keywords": ["banking", "insurance", "lending",
                           "credit", "underwriting"],
        "ai_exposure": 6,
    },
    "Consulting": {
        "keywords": ["consultant", "management consultant",
                     "strategy consultant", "advisory",
                     "functional consultant", "technical consultant"],
        "skill_keywords": ["consulting", "strategy", "advisory"],
        "ai_exposure": 5,
    },
    "Hospitality & Travel": {
        "keywords": ["hotel", "hospitality", "chef", "cook", "restaurant",
                     "front desk", "travel", "tourism", "housekeeping",
                     "food and beverage", "bartender", "waiter"],
        "skill_keywords": ["hospitality", "hotel management", "food service"],
        "ai_exposure": 2,
    },
    "Drivers & Delivery": {
        "keywords": ["driver", "delivery", "courier", "rider", "fleet", "transport"],
        "skill_keywords": ["driving", "delivery", "logistics"],
        "ai_exposure": 2,
    },
    "Skilled Trades & Technicians": {
        "keywords": ["technician", "fitter", "welder", "plumber",
                     "electrician", "machinist", "fire and safety",
                     "safety officer", "hvac"],
        "skill_keywords": ["welding", "fitting", "plumbing", "hvac"],
        "ai_exposure": 1,
    },
    "Executive Assistants & Admin": {
        "keywords": ["executive assistant", "admin", "office assistant",
                     "receptionist", "secretary", "front office",
                     "administrative", "back office", "office coordinator",
                     "assistant manager"],
        "skill_keywords": ["ms office", "administrative", "data entry", "filing"],
        "ai_exposure": 8,
    },
    "Pharmacy & Life Sciences": {
        "keywords": ["pharma", "pharmaceutical", "biotech",
                     "microbiology", "chemistry", "biochemistry",
                     "formulation", "drug", "regulatory affairs",
                     "medical representative"],
        "skill_keywords": ["pharmaceutical", "formulation",
                           "drug regulatory", "gmp"],
        "ai_exposure": 4,
    },
}


def categorize_job(title, skills):
    """Assign a job to the best-matching category."""
    title_lower = title.lower()
    skills_lower = skills.lower() if skills else ""
    best_cat = None
    best_score = 0
    for cat, info in NAUKRI_CATEGORIES.items():
        score = 0
        for kw in info["keywords"]:
            if kw in title_lower:
                score += 10 + len(kw)
        for kw in info.get("skill_keywords", []):
            if kw in skills_lower:
                score += 2
        if score > best_score:
            best_score = score
            best_cat = cat
    if best_cat is None or best_score < 5:
        for cat, info in NAUKRI_CATEGORIES.items():
            score = 0
            for kw in info.get("skill_keywords", []):
                if kw in skills_lower:
                    score += 3
            if score > best_score:
                best_score = score
                best_cat = cat
    return best_cat or "Other"


def get_pay_bracket(avg_salary_lakhs):
    if avg_salary_lakhs < 3:
        return "<3L"
    elif avg_salary_lakhs < 6:
        return "3-6L"
    elif avg_salary_lakhs < 10:
        return "6-10L"
    elif avg_salary_lakhs < 20:
        return "10-20L"
    else:
        return "20L+"


def get_experience_bracket(avg_exp):
    if avg_exp <= 1:
        return "Fresher (0-1)"
    elif avg_exp <= 3:
        return "Junior (1-3)"
    elif avg_exp <= 7:
        return "Mid (3-7)"
    elif avg_exp <= 12:
        return "Senior (7-12)"
    else:
        return "Expert (12+)"


def main():
    print("=" * 70)
    print("  Indian Job Market × AI Exposure — Expanded Preprocessor")
    print("  Source: Naukri.com (organized) + PLFS 2023-24 (full economy)")
    print("=" * 70)

    # ── Phase 1: Process Naukri.com CSV data ──
    print("\n[Phase 1] Processing Naukri.com CSV (organized sector)...")
    jobs_by_category = defaultdict(list)

    with open('indian-job-market-dataset-2025.csv', 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            title = row.get('title', '').strip()
            skills = row.get('tagsAndSkills', '')
            location = row.get('location', '').strip()
            try:
                min_sal = int(row.get('minimumSalary', '0') or '0')
                max_sal = int(row.get('maximumSalary', '0') or '0')
            except:
                min_sal = max_sal = 0
            try:
                min_exp = int(row.get('minimumExperience', '0') or '0')
                max_exp = int(row.get('maximumExperience', '0') or '0')
            except:
                min_exp = max_exp = 0

            avg_sal = 0
            if min_sal > 0 and max_sal > 0:
                avg_sal = (min_sal + max_sal) / 2
            elif max_sal > 0:
                avg_sal = max_sal
            elif min_sal > 0:
                avg_sal = min_sal
            avg_exp = (min_exp + max_exp) / 2
            cat = categorize_job(title, skills)
            city = location.split(',')[0].strip() if location else "Unknown"
            jobs_by_category[cat].append({
                "avg_salary": avg_sal,
                "avg_experience": avg_exp,
                "city": city,
            })

    naukri_total = sum(len(v) for v in jobs_by_category.values())
    print(f"  → {naukri_total:,} listings across {len(jobs_by_category)} categories")

    # ── Phase 2: Build category-level aggregates from Naukri data ──
    print("\n[Phase 2] Computing organized-sector aggregates...")
    categories_data = []

    for cat, jobs in sorted(jobs_by_category.items(), key=lambda x: -len(x[1])):
        count = len(jobs)
        ai_score = NAUKRI_CATEGORIES.get(cat, {}).get("ai_exposure", 5)
        salaries = [j["avg_salary"] for j in jobs if j["avg_salary"] > 0]
        avg_salary = statistics.mean(salaries) if salaries else 0
        experiences = [j["avg_experience"] for j in jobs]
        avg_exp = statistics.mean(experiences) if experiences else 0

        pay_dist = defaultdict(int)
        for j in jobs:
            if j["avg_salary"] > 0:
                pay_dist[get_pay_bracket(j["avg_salary"] / 100000)] += 1

        exp_dist = defaultdict(int)
        for j in jobs:
            exp_dist[get_experience_bracket(j["avg_experience"])] += 1

        city_dist = defaultdict(int)
        for j in jobs:
            city_dist[j["city"]] += 1
        top_cities = sorted(city_dist.items(), key=lambda x: -x[1])[:5]

        # Scale: Naukri listings represent organized sector.
        # Multiply by an estimated ratio to get actual workforce numbers.
        # Organized sector ≈ 42M workers, Naukri has ~98K listings.
        # Scale factor ≈ 42M / 98K ≈ 428
        ORGANIZED_SCALE = 428
        estimated_workers = count * ORGANIZED_SCALE

        categories_data.append({
            "name": cat,
            "count": estimated_workers,
            "naukri_listings": count,
            "ai_exposure": ai_score,
            "avg_salary_lakhs": round(avg_salary / 100000, 1),
            "avg_experience": round(avg_exp, 1),
            "pay_distribution": dict(pay_dist),
            "experience_distribution": dict(exp_dist),
            "top_cities": top_cities,
            "sector": "organized",
            "data_source": "naukri",
        })

    # ── Phase 3: Add PLFS informal-sector categories ──
    print("\n[Phase 3] Adding PLFS informal-sector workforce estimates...")

    for cat_name, info in PLFS_CATEGORIES.items():
        workers = info["workers_millions"] * 1_000_000
        avg_sal_lakhs = info["avg_salary_lakhs"]

        # Build simplified pay and experience distributions
        pay_dist = {}
        if avg_sal_lakhs < 1.5:
            pay_dist = {"<3L": int(workers * 0.95), "3-6L": int(workers * 0.05)}
        elif avg_sal_lakhs < 3:
            pay_dist = {"<3L": int(workers * 0.6), "3-6L": int(workers * 0.35), "6-10L": int(workers * 0.05)}
        elif avg_sal_lakhs < 6:
            pay_dist = {"<3L": int(workers * 0.2), "3-6L": int(workers * 0.5), "6-10L": int(workers * 0.25), "10-20L": int(workers * 0.05)}
        else:
            pay_dist = {"3-6L": int(workers * 0.3), "6-10L": int(workers * 0.4), "10-20L": int(workers * 0.25), "20L+": int(workers * 0.05)}

        avg_exp = info["avg_experience"]
        exp_dist = {}
        if avg_exp <= 5:
            exp_dist = {"Fresher (0-1)": int(workers * 0.15), "Junior (1-3)": int(workers * 0.25), "Mid (3-7)": int(workers * 0.35), "Senior (7-12)": int(workers * 0.2), "Expert (12+)": int(workers * 0.05)}
        elif avg_exp <= 10:
            exp_dist = {"Fresher (0-1)": int(workers * 0.05), "Junior (1-3)": int(workers * 0.1), "Mid (3-7)": int(workers * 0.25), "Senior (7-12)": int(workers * 0.35), "Expert (12+)": int(workers * 0.25)}
        else:
            exp_dist = {"Fresher (0-1)": int(workers * 0.03), "Junior (1-3)": int(workers * 0.07), "Mid (3-7)": int(workers * 0.2), "Senior (7-12)": int(workers * 0.3), "Expert (12+)": int(workers * 0.4)}

        categories_data.append({
            "name": cat_name,
            "count": int(workers),
            "naukri_listings": 0,
            "ai_exposure": info["ai_exposure"],
            "avg_salary_lakhs": avg_sal_lakhs,
            "avg_experience": avg_exp,
            "pay_distribution": pay_dist,
            "experience_distribution": exp_dist,
            "top_cities": info["top_cities"],
            "sector": info.get("sector", "informal"),
            "data_source": "plfs",
            "description": info.get("description", ""),
            "source_note": info.get("source", ""),
        })
        print(f"  + {cat_name}: {info['workers_millions']}M workers, AI exposure {info['ai_exposure']}/10")

    # ── Phase 4: Compute overall statistics ──
    print("\n[Phase 4] Computing overall statistics...")
    total_workers = sum(c["count"] for c in categories_data)

    # Add percentage
    for c in categories_data:
        c["percentage"] = round(c["count"] / total_workers * 100, 2)

    # Weighted average exposure
    total_weighted = sum(c["ai_exposure"] * c["count"] for c in categories_data)
    weighted_avg = round(total_weighted / total_workers, 1) if total_workers > 0 else 0

    # Exposure breakdown
    exposure_brackets = {
        "Minimal (0-1)": 0,
        "Low (2-3)": 0,
        "Moderate (4-5)": 0,
        "High (6-7)": 0,
        "Very high (8-10)": 0,
    }
    for cat_data in categories_data:
        score = cat_data["ai_exposure"]
        count = cat_data["count"]
        if score <= 1:
            exposure_brackets["Minimal (0-1)"] += count
        elif score <= 3:
            exposure_brackets["Low (2-3)"] += count
        elif score <= 5:
            exposure_brackets["Moderate (4-5)"] += count
        elif score <= 7:
            exposure_brackets["High (6-7)"] += count
        else:
            exposure_brackets["Very high (8-10)"] += count

    # Exposure by pay
    pay_brackets_order = ["<3L", "3-6L", "6-10L", "10-20L", "20L+"]
    exposure_by_pay = {}
    for pb in pay_brackets_order:
        total_w = 0
        total_s = 0
        for cat_data in categories_data:
            n = cat_data.get("pay_distribution", {}).get(pb, 0)
            if n > 0:
                total_w += n
                total_s += cat_data["ai_exposure"] * n
        exposure_by_pay[pb] = round(total_s / total_w, 1) if total_w > 0 else 0

    # Exposure by experience
    exp_brackets_order = ["Fresher (0-1)", "Junior (1-3)", "Mid (3-7)", "Senior (7-12)", "Expert (12+)"]
    exposure_by_exp = {}
    for eb in exp_brackets_order:
        total_w = 0
        total_s = 0
        for cat_data in categories_data:
            n = cat_data.get("experience_distribution", {}).get(eb, 0)
            if n > 0:
                total_w += n
                total_s += cat_data["ai_exposure"] * n
        exposure_by_exp[eb] = round(total_s / total_w, 1) if total_w > 0 else 0

    # Total wages exposed (high AI score >= 7)
    total_wages_exposed = 0
    for c in categories_data:
        if c["ai_exposure"] >= 7:
            total_wages_exposed += c["count"] * c["avg_salary_lakhs"] * 100000
    wages_exposed_cr = round(total_wages_exposed / 10000000, 0)

    # Total workers in organized vs informal
    organized_workers = sum(c["count"] for c in categories_data if c["data_source"] == "naukri")
    informal_workers = sum(c["count"] for c in categories_data if c["data_source"] == "plfs")

    # Format total
    if total_workers >= 1_000_000_000:
        total_formatted = f"{total_workers / 1_000_000_000:.2f}B"
    elif total_workers >= 1_000_000:
        total_formatted = f"{total_workers / 1_000_000:.0f}M"
    else:
        total_formatted = f"{total_workers / 1000:.0f}K"

    output = {
        "total_workers": total_workers,
        "total_workers_formatted": total_formatted,
        "organized_sector_workers": organized_workers,
        "informal_sector_workers": informal_workers,
        "naukri_listings": naukri_total,
        "weighted_avg_exposure": weighted_avg,
        "exposure_breakdown": exposure_brackets,
        "exposure_by_pay": exposure_by_pay,
        "exposure_by_experience": exposure_by_exp,
        "wages_exposed_crores": wages_exposed_cr,
        "data_sources": [
            "Naukri.com Indian Job Market Dataset 2025 (97,929 listings)",
            "PLFS Annual Report July 2023 – June 2024 (MoSPI/NSO)",
            "Wikipedia Economy of India (citing PLFS, ILO, World Bank, IBEF)",
            "PIB press releases, Economic Survey 2024-25",
        ],
        "categories": sorted(categories_data, key=lambda x: -x["count"]),
    }

    with open('job_data.json', 'w') as f:
        json.dump(output, f, indent=2)

    # ── Summary ──
    print(f"\n{'=' * 70}")
    print(f"  RESULTS")
    print(f"{'=' * 70}")
    print(f"  Total workforce:       {total_workers / 1_000_000:.0f}M workers")
    print(f"  Organized sector:      {organized_workers / 1_000_000:.0f}M (from {naukri_total:,} Naukri listings × 428)")
    print(f"  Informal sector:       {informal_workers / 1_000_000:.0f}M (PLFS 2023-24)")
    print(f"  Categories:            {len(categories_data)}")
    print(f"  Weighted avg exposure: {weighted_avg}/10")
    print(f"  Wages exposed (7+):    ₹{wages_exposed_cr:,.0f} Cr")
    print()
    print(f"  {'Category':<40s}  {'Workers':>12s}  AI  {'Avg ₹':>8s}")
    print(f"  {'-' * 40}  {'-' * 12}  --  {'-' * 8}")
    for c in sorted(categories_data, key=lambda x: -x["count"])[:20]:
        w = c["count"]
        if w >= 1_000_000:
            w_str = f"{w / 1_000_000:.1f}M"
        elif w >= 1_000:
            w_str = f"{w / 1_000:.0f}K"
        else:
            w_str = str(w)
        print(f"  {c['name']:<40s}  {w_str:>12s}  {c['ai_exposure']:2d}  {c['avg_salary_lakhs']:>7.1f}L")


if __name__ == "__main__":
    main()
