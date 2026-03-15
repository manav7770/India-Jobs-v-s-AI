"""
Preprocessor: reads the Indian job market CSV, categorizes jobs into broad categories,
assigns AI exposure scores, and generates a JSON data file for the treemap visualization.
"""
import csv
import re
import json
from collections import defaultdict
import statistics

# ── Job categories with keyword matching rules and AI exposure scores ──
# AI Exposure Score: 0 (no exposure) to 10 (fully automatable by AI)
# Based on how much of the job's core tasks can be performed or significantly
# assisted by current/near-future AI systems (LLMs, vision models, code gen, etc.)

CATEGORIES = {
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
        "skill_keywords": ["sales", "business development", "crm",
                           "lead generation"],
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
        "skill_keywords": ["teaching", "training", "education",
                           "curriculum"],
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
        "skill_keywords": ["embedded", "vlsi", "plc", "scada",
                           "circuit design"],
        "ai_exposure": 3,
    },
    "Legal & Compliance": {
        "keywords": ["legal", "lawyer", "advocate", "compliance",
                     "company secretary", "legal counsel", "paralegal",
                     "regulatory", "law", "litigation"],
        "skill_keywords": ["legal", "compliance", "regulatory",
                           "litigation"],
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
        "skill_keywords": ["hospitality", "hotel management",
                           "food service"],
        "ai_exposure": 2,
    },
    "Drivers & Delivery": {
        "keywords": ["driver", "delivery", "courier", "rider",
                     "fleet", "transport"],
        "skill_keywords": ["driving", "delivery", "logistics"],
        "ai_exposure": 2,
    },
    "Skilled Trades & Technicians": {
        "keywords": ["technician", "fitter", "welder", "plumber",
                     "electrician", "machinist", "fire and safety",
                     "safety officer", "hvac"],
        "skill_keywords": ["welding", "fitting", "plumbing",
                           "hvac"],
        "ai_exposure": 1,
    },
    "Executive Assistants & Admin": {
        "keywords": ["executive assistant", "admin", "office assistant",
                     "receptionist", "secretary", "front office",
                     "administrative", "back office", "office coordinator",
                     "assistant manager"],
        "skill_keywords": ["ms office", "administrative", "data entry",
                           "filing"],
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

    for cat, info in CATEGORIES.items():
        score = 0
        # Title keyword matches are strongest signal
        for kw in info["keywords"]:
            if kw in title_lower:
                score += 10 + len(kw)  # longer matches = better
        # Skill keyword matches are secondary signal
        for kw in info.get("skill_keywords", []):
            if kw in skills_lower:
                score += 2
        if score > best_score:
            best_score = score
            best_cat = cat

    if best_cat is None or best_score < 5:
        # Try skills-only matching as fallback
        for cat, info in CATEGORIES.items():
            score = 0
            for kw in info.get("skill_keywords", []):
                if kw in skills_lower:
                    score += 3
            if score > best_score:
                best_score = score
                best_cat = cat

    return best_cat or "Other"


def get_pay_bracket(avg_salary_lakhs):
    """Assign salary bracket in Lakhs PA."""
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
    """Assign experience bracket."""
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
    # Read all jobs
    jobs_by_category = defaultdict(list)
    all_jobs = []

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

            job = {
                "title": title,
                "category": cat,
                "city": city,
                "avg_salary": avg_sal,
                "avg_experience": avg_exp,
                "skills": skills,
            }
            jobs_by_category[cat].append(job)
            all_jobs.append(job)

    # Build category-level aggregates
    categories_data = []
    total_jobs = len(all_jobs)
    total_wages_exposed = 0  # sum of avg salaries for high-exposure (7+) jobs

    for cat, jobs in sorted(jobs_by_category.items(), key=lambda x: -len(x[1])):
        count = len(jobs)
        ai_score = CATEGORIES.get(cat, {}).get("ai_exposure", 5)

        salaries = [j["avg_salary"] for j in jobs if j["avg_salary"] > 0]
        avg_salary = statistics.mean(salaries) if salaries else 0

        experiences = [j["avg_experience"] for j in jobs]
        avg_exp = statistics.mean(experiences) if experiences else 0

        # Salary distribution
        pay_dist = defaultdict(int)
        for j in jobs:
            if j["avg_salary"] > 0:
                bracket = get_pay_bracket(j["avg_salary"] / 100000)
                pay_dist[bracket] += 1

        # Experience distribution
        exp_dist = defaultdict(int)
        for j in jobs:
            bracket = get_experience_bracket(j["avg_experience"])
            exp_dist[bracket] += 1

        # City distribution (top 5)
        city_dist = defaultdict(int)
        for j in jobs:
            city_dist[j["city"]] += 1
        top_cities = sorted(city_dist.items(), key=lambda x: -x[1])[:5]

        if ai_score >= 7:
            total_wages_exposed += sum(salaries)

        categories_data.append({
            "name": cat,
            "count": count,
            "ai_exposure": ai_score,
            "avg_salary_lakhs": round(avg_salary / 100000, 1),
            "avg_experience": round(avg_exp, 1),
            "pay_distribution": dict(pay_dist),
            "experience_distribution": dict(exp_dist),
            "top_cities": top_cities,
            "percentage": round(count / total_jobs * 100, 1),
        })

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
        scores = []
        for cat_data in categories_data:
            cat_name = cat_data["name"]
            cat_score = cat_data["ai_exposure"]
            cat_pay = cat_data.get("pay_distribution", {})
            n = cat_pay.get(pb, 0)
            scores.extend([cat_score] * n)
        if scores:
            exposure_by_pay[pb] = round(statistics.mean(scores), 1)
        else:
            exposure_by_pay[pb] = 0

    # Exposure by experience
    exp_brackets_order = ["Fresher (0-1)", "Junior (1-3)", "Mid (3-7)", "Senior (7-12)", "Expert (12+)"]
    exposure_by_exp = {}
    for eb in exp_brackets_order:
        scores = []
        for cat_data in categories_data:
            cat_name = cat_data["name"]
            cat_score = cat_data["ai_exposure"]
            cat_exp = cat_data.get("experience_distribution", {})
            n = cat_exp.get(eb, 0)
            scores.extend([cat_score] * n)
        if scores:
            exposure_by_exp[eb] = round(statistics.mean(scores), 1)
        else:
            exposure_by_exp[eb] = 0

    # Weighted average exposure
    total_weighted = sum(c["ai_exposure"] * c["count"] for c in categories_data)
    weighted_avg = round(total_weighted / total_jobs, 1) if total_jobs > 0 else 0

    # Total wages exposed (in Crores)
    wages_exposed_cr = round(total_wages_exposed / 10000000, 0)

    output = {
        "total_jobs": total_jobs,
        "total_jobs_formatted": f"{total_jobs/1000:.0f}K" if total_jobs < 1000000 else f"{total_jobs/1000000:.1f}M",
        "weighted_avg_exposure": weighted_avg,
        "exposure_breakdown": exposure_brackets,
        "exposure_by_pay": exposure_by_pay,
        "exposure_by_experience": exposure_by_exp,
        "wages_exposed_crores": wages_exposed_cr,
        "categories": sorted(categories_data, key=lambda x: -x["count"]),
    }

    with open('job_data.json', 'w') as f:
        json.dump(output, f, indent=2)

    print(f"Processed {total_jobs} jobs into {len(categories_data)} categories")
    print(f"Weighted avg AI exposure: {weighted_avg}/10")
    print(f"Total wages exposed (7+ score): ₹{wages_exposed_cr} Cr")
    print()
    for c in sorted(categories_data, key=lambda x: -x["count"]):
        print(f"  {c['name']:40s}  {c['count']:6d} jobs  AI:{c['ai_exposure']}/10  Avg ₹{c['avg_salary_lakhs']}L")


if __name__ == "__main__":
    main()
