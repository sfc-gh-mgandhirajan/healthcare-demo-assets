import os
from fpdf import FPDF

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "synthetic_docs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

TRIALS = {
    "ALLIANCE": {
        "device": "Edwards SAPIEN X4 Transcatheter Heart Valve System",
        "indication": "symptomatic, severe, calcific aortic stenosis",
        "nct": "NCT05172973",
        "sponsor": "Edwards Lifesciences Corporation",
        "pi_global": "Dr. Michael Reardon",
        "design": "prospective, multicenter, non-randomized",
        "population": "patients with symptomatic severe aortic stenosis who are candidates for transcatheter aortic valve replacement",
        "primary_endpoint": "all-cause mortality or disabling stroke at 1 year",
        "secondary_endpoints": [
            "device success at 30 days (VARC-3 criteria)",
            "mean aortic valve gradient at 1 year",
            "effective orifice area (EOA) at 1 year",
            "rate of moderate or greater paravalvular leak at 30 days",
            "NYHA functional class improvement at 1 year",
            "new permanent pacemaker implantation at 30 days",
        ],
        "inclusion": [
            "Age >= 18 years",
            "Severe calcific aortic stenosis defined as: aortic valve area (AVA) < 1.0 cm2 or indexed AVA < 0.6 cm2/m2, AND mean gradient >= 40 mmHg or peak velocity >= 4.0 m/s",
            "NYHA Functional Class II or greater",
            "Heart Team consensus that the patient is appropriate for TAVR",
            "Aortic annulus diameter 18-29 mm by CT assessment",
        ],
        "exclusion": [
            "Unicuspid or bicuspid aortic valve confirmed by imaging",
            "Pre-existing prosthetic heart valve or ring in any position",
            "Severe mitral regurgitation (4+) or severe tricuspid regurgitation (4+)",
            "LVEF < 20%",
            "Hemodynamic instability requiring inotropic or mechanical circulatory support",
            "Active endocarditis or sepsis",
            "Stroke or TIA within 6 months prior to enrollment",
            "Estimated life expectancy < 24 months due to non-cardiac comorbidities",
        ],
        "sites": {
            "Site-001": {"name": "Cleveland Clinic", "pi": "Dr. Samir Kapadia", "city": "Cleveland, OH"},
            "Site-002": {"name": "Mayo Clinic", "pi": "Dr. Vuyisile Nkomo", "city": "Rochester, MN"},
        },
    },
    "ENCIRCLE": {
        "device": "Edwards SAPIEN M3 Transcatheter Mitral Valve Replacement System",
        "indication": "symptomatic severe mitral regurgitation (3+ or 4+)",
        "nct": "NCT04153292",
        "sponsor": "Edwards Lifesciences Corporation",
        "pi_global": "Dr. Vinod Thourani",
        "design": "prospective, multicenter, single-arm",
        "population": "patients with symptomatic severe mitral regurgitation for whom surgical and transcatheter treatment options are deemed unsuitable",
        "primary_endpoint": "all-cause mortality at 30 days",
        "secondary_endpoints": [
            "reduction in mitral regurgitation to trace/mild at 30 days",
            "NYHA functional class improvement at 6 months",
            "Kansas City Cardiomyopathy Questionnaire (KCCQ) improvement at 6 months",
            "rate of device migration or embolization at 30 days",
            "hemolysis requiring intervention at 30 days",
        ],
        "inclusion": [
            "Age >= 18 years",
            "Symptomatic severe mitral regurgitation (3+ or 4+) by echocardiography",
            "NYHA Functional Class II, III, or ambulatory IV",
            "Heart Team determination that patient is not a candidate for surgical mitral valve repair or replacement",
            "Prior failed transcatheter edge-to-edge repair (TEER) OR deemed unsuitable for TEER (Failed TEER Registry)",
            "Mitral annular diameter 27-38 mm by CT assessment",
        ],
        "exclusion": [
            "Severe mitral annular calcification (MAC) precluding device implantation",
            "LVEF < 20%",
            "Severe tricuspid regurgitation requiring intervention",
            "Prior mitral valve surgery or transcatheter mitral valve replacement",
            "Severe pulmonary hypertension (PASP > 70 mmHg)",
            "Active endocarditis or bacteremia",
            "Renal failure requiring dialysis or eGFR < 20 mL/min/1.73m2",
        ],
        "sites": {
            "Site-003": {"name": "Cedars-Sinai Medical Center", "pi": "Dr. Raj Makkar", "city": "Los Angeles, CA"},
            "Site-004": {"name": "Mount Sinai Hospital", "pi": "Dr. Samin Sharma", "city": "New York, NY"},
        },
    },
    "TRISCEND_II": {
        "device": "Edwards EVOQUE Transcatheter Tricuspid Valve Replacement System",
        "indication": "symptomatic severe or greater tricuspid regurgitation",
        "nct": "NCT04482062",
        "sponsor": "Edwards Lifesciences Corporation",
        "pi_global": "Dr. Georg Nickenig",
        "design": "prospective, multicenter, randomized, controlled",
        "population": "patients with symptomatic severe or greater tricuspid regurgitation",
        "primary_endpoint": "hierarchical composite of all-cause death, tricuspid valve surgery, hospitalization for heart failure at 1 year",
        "secondary_endpoints": [
            "reduction in tricuspid regurgitation to moderate or less at 30 days",
            "NYHA functional class improvement at 1 year",
            "6-minute walk distance improvement at 1 year",
            "KCCQ overall summary score change at 1 year",
            "right ventricular remodeling (RV end-diastolic volume index) at 1 year",
        ],
        "inclusion": [
            "Age >= 18 years",
            "Severe or greater tricuspid regurgitation (>=3+) by echocardiography",
            "NYHA Functional Class II or greater despite optimal medical therapy",
            "Tricuspid annulus diameter <= 55 mm by CT assessment",
            "Heart Team agreement that the patient may benefit from tricuspid valve intervention",
        ],
        "exclusion": [
            "Severe unrepaired left-sided valve disease requiring intervention",
            "Right ventricular failure requiring inotropic support",
            "Severe right ventricular dysfunction (TAPSE < 13 mm)",
            "Prior tricuspid valve surgery or transcatheter replacement",
            "Implanted cardiac device with leads that would interfere with valve placement",
            "Systolic pulmonary artery pressure > 60 mmHg",
            "eGFR < 25 mL/min/1.73m2",
        ],
        "sites": {
            "Site-005": {"name": "Massachusetts General Hospital", "pi": "Dr. Jonathan Passeri", "city": "Boston, MA"},
            "Site-006": {"name": "Stanford University Medical Center", "pi": "Dr. William Hiesinger", "city": "Stanford, CA"},
        },
    },
    "ALT_FLOW_II": {
        "device": "Edwards APTURE Transcatheter Interatrial Shunt System",
        "indication": "heart failure with preserved or mildly reduced ejection fraction (HFpEF/HFmrEF)",
        "nct": "NCT05686317",
        "sponsor": "Edwards Lifesciences Corporation",
        "pi_global": "Dr. Scott Solomon",
        "design": "prospective, multicenter, randomized, double-blinded, sham-controlled",
        "population": "patients with HFpEF or HFmrEF (LVEF > 40%) with elevated left atrial pressure",
        "primary_endpoint": "recurrent heart failure events or cardiovascular death at 12 months",
        "secondary_endpoints": [
            "change in PCWP at rest and exercise at 3 months",
            "KCCQ overall summary score change at 6 months",
            "6-minute walk distance change at 6 months",
            "NT-proBNP change at 6 months",
            "heart failure hospitalization rate at 12 months",
        ],
        "inclusion": [
            "Age >= 40 years",
            "Heart failure with LVEF > 40%",
            "NYHA Functional Class II-IV (ambulatory)",
            "Elevated PCWP >= 25 mmHg during supine bicycle exercise, or >= 15 mmHg at rest",
            "On stable guideline-directed medical therapy (GDMT) for >= 30 days",
            "At least one heart failure hospitalization or IV diuretic administration in past 12 months",
        ],
        "exclusion": [
            "Significant interatrial septal abnormality (PFO, ASD, or prior closure device)",
            "Severe right ventricular dysfunction",
            "Severe pulmonary hypertension (PVR > 4 Wood units)",
            "Constrictive pericarditis or restrictive cardiomyopathy",
            "Severe COPD (FEV1 < 30% predicted)",
            "BMI > 50 kg/m2",
            "eGFR < 25 mL/min/1.73m2",
        ],
        "sites": {},
    },
    "CLASP_IID": {
        "device": "Edwards PASCAL Transcatheter Valve Repair System",
        "indication": "degenerative mitral regurgitation",
        "nct": "NCT03706833",
        "sponsor": "Edwards Lifesciences Corporation",
        "pi_global": "Dr. Paul Sorajja",
        "design": "prospective, multicenter, randomized, controlled",
        "population": "patients with symptomatic degenerative mitral regurgitation (3+ or 4+)",
        "primary_endpoint": "composite of freedom from device-related complications and reduction of MR to 2+ or less at 2 years",
        "secondary_endpoints": [
            "MR reduction to 1+ or less at 30 days",
            "NYHA functional class improvement at 1 year",
            "freedom from mitral valve surgery at 2 years",
            "KCCQ change from baseline at 1 year",
            "left ventricular reverse remodeling (LVEDV change) at 1 year",
        ],
        "inclusion": [
            "Age >= 18 years",
            "Symptomatic severe degenerative mitral regurgitation (3+ or 4+)",
            "NYHA Functional Class II, III, or ambulatory IV",
            "Heart Team determination that patient is suitable for transcatheter mitral valve repair",
            "Primary mitral regurgitation mechanism is degenerative (prolapse, flail)",
        ],
        "exclusion": [
            "Functional/secondary mitral regurgitation as primary etiology",
            "Heavily calcified mitral annulus or leaflets",
            "LVEF < 25%",
            "Mitral valve orifice area < 4.0 cm2",
            "Prior mitral valve surgery or TEER procedure",
            "Active endocarditis",
            "Hemodynamic instability",
        ],
        "sites": {},
    },
}


class TrialPDF(FPDF):
    def __init__(self, trial_name, doc_type, version):
        super().__init__()
        self.trial_name = trial_name
        self.doc_type = doc_type
        self.version = version

    def header(self):
        self.set_fill_color(220, 50, 50)
        self.set_font("Helvetica", "B", 7)
        self.set_text_color(255, 255, 255)
        self.cell(0, 4, "SYNTHETIC DATA - FOR DEMO PURPOSES ONLY - NOT REAL CLINICAL DATA", align="C", fill=True)
        self.ln(2)
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(100, 100, 100)
        self.cell(0, 5, f"{self.trial_name} | {self.doc_type} | {self.version}", align="R")
        self.ln(8)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-20)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(180, 50, 50)
        self.cell(0, 5, "SYNTHETIC / FICTITIOUS DATA - For demo purposes only. Not real clinical trial documentation.", align="C")
        self.ln(4)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}} | {self.trial_name}", align="C")

    def section_title(self, title):
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(0, 51, 102)
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT")
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def subsection_title(self, title):
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(0, 76, 153)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(0, 0, 0)
        self.multi_cell(0, 5, text)
        self.ln(2)

    def bullet_list(self, items):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(0, 0, 0)
        for item in items:
            self.cell(5)
            self.cell(5, 5, "-")
            self.multi_cell(175, 5, item)
            self.ln(1)
        self.ln(2)


def add_disclaimer_page(pdf):
    pdf.add_page()
    pdf.ln(15)
    pdf.set_draw_color(200, 40, 40)
    pdf.set_line_width(0.8)
    box_x = 15
    box_y = pdf.get_y()
    box_w = 180
    pdf.set_x(box_x + 8)
    pdf.set_font("Helvetica", "B", 22)
    pdf.set_text_color(200, 40, 40)
    pdf.cell(box_w - 16, 14, "SYNTHETIC DATA DISCLAIMER", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)
    pdf.set_x(box_x + 10)
    pdf.set_font("Helvetica", "B", 12)
    pdf.set_text_color(50, 50, 50)
    pdf.multi_cell(box_w - 20, 7, "This document contains entirely SYNTHETIC / FICTITIOUS data generated for demonstration purposes only.")
    pdf.ln(4)
    items = [
        "This is NOT a real clinical trial document.",
        "All patient data, site information, investigator names, endpoints, and results are fictitious.",
        "Trial names and device names reference real Edwards Lifesciences products but all document content is fabricated.",
        "NCT numbers reference real ClinicalTrials.gov registrations but the protocol details herein are synthetic.",
        "This document was created for demo purposes only.",
        "No regulatory, clinical, or business decisions should be made based on this content.",
    ]
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(70, 70, 70)
    for item in items:
        pdf.set_x(box_x + 12)
        pdf.cell(4, 5, "-")
        pdf.multi_cell(box_w - 28, 5, item)
        pdf.ln(1)
    pdf.ln(6)
    pdf.set_x(box_x + 10)
    pdf.set_font("Helvetica", "I", 9)
    pdf.set_text_color(140, 140, 140)
    pdf.cell(box_w - 20, 8, "For demonstration purposes only", align="C")
    pdf.ln(8)
    box_h = pdf.get_y() - box_y
    pdf.rect(box_x, box_y, box_w, box_h)


def generate_protocol(trial_key, trial, version, amendment_num=None):
    trial_display = trial_key.replace("_", " ")
    pdf = TrialPDF(trial_display, "Clinical Investigation Plan", version)
    pdf.alias_nb_pages()
    add_disclaimer_page(pdf)
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(0, 51, 102)
    pdf.ln(10)
    pdf.cell(0, 10, f"REGULATORY SUBMISSION", align="C", new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("Helvetica", "B", 18)
    pdf.set_text_color(0, 51, 102)
    pdf.ln(10)
    pdf.cell(0, 12, f"CLINICAL INVESTIGATION PLAN", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    pdf.set_font("Helvetica", "B", 15)
    pdf.cell(0, 10, f"The {trial_display} Clinical Trial", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    pdf.set_font("Helvetica", "", 12)
    pdf.set_text_color(50, 50, 50)
    pdf.cell(0, 8, f"Protocol Version: {version}", align="C", new_x="LMARGIN", new_y="NEXT")
    if amendment_num:
        pdf.cell(0, 8, f"Amendment {amendment_num}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, f"ClinicalTrials.gov: {trial['nct']}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, f"Sponsor: {trial['sponsor']}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, f"Global Principal Investigator: {trial['pi_global']}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)

    pdf.add_page()
    pdf.section_title("1. SYNOPSIS")
    pdf.body_text(f"Study Title: A {trial['design']} clinical trial to evaluate the safety and effectiveness of the {trial['device']} for the treatment of patients with {trial['indication']}.")
    pdf.body_text(f"Study Population: {trial['population']}.")
    pdf.body_text(f"Primary Endpoint: {trial['primary_endpoint']}.")

    pdf.section_title("2. STUDY OBJECTIVES")
    pdf.subsection_title("2.1 Primary Objective")
    pdf.body_text(f"To evaluate the safety and effectiveness of the {trial['device']} in {trial['population']}. The primary endpoint is {trial['primary_endpoint']}.")
    pdf.subsection_title("2.2 Secondary Objectives")
    pdf.bullet_list(trial["secondary_endpoints"])

    pdf.add_page()
    pdf.section_title("3. STUDY DESIGN")
    pdf.body_text(f"This is a {trial['design']} clinical trial. The study is designed to evaluate the {trial['device']} in patients with {trial['indication']}.")
    pdf.body_text(f"Subjects who meet all inclusion criteria and none of the exclusion criteria will be enrolled. All enrolled subjects will undergo the index procedure and will be followed for a minimum of 5 years post-procedure.")
    pdf.body_text("Follow-up visits will occur at 30 days, 6 months, 1 year, and annually thereafter through 5 years. Echocardiographic assessment will be performed at each follow-up visit by an independent core laboratory.")

    pdf.section_title("4. STUDY POPULATION")
    pdf.subsection_title("4.1 Inclusion Criteria")
    pdf.bullet_list(trial["inclusion"])
    pdf.subsection_title("4.2 Exclusion Criteria")
    pdf.bullet_list(trial["exclusion"])

    pdf.add_page()
    pdf.section_title("5. STUDY PROCEDURES")
    pdf.subsection_title("5.1 Screening")
    pdf.body_text("All potential subjects will undergo a comprehensive screening evaluation including transthoracic echocardiography (TTE), electrocardiogram (ECG), multi-slice computed tomography (MSCT) of the heart and peripheral vasculature, laboratory assessments (CBC, BMP, coagulation panel, NT-proBNP), and a 6-minute walk test (6MWT).")
    pdf.subsection_title("5.2 Index Procedure")
    pdf.body_text(f"The {trial['device']} will be implanted under general anesthesia or conscious sedation per institutional practice. The procedure will be performed in a hybrid operating room or cardiac catheterization laboratory equipped with fluoroscopy and transesophageal echocardiography (TEE) guidance.")
    pdf.subsection_title("5.3 Post-Procedure Monitoring")
    pdf.body_text("Subjects will be monitored in the cardiac care unit (CCU) or intensive care unit (ICU) for a minimum of 24 hours post-procedure. Transthoracic echocardiography will be performed prior to discharge. ECG rhythm monitoring will continue until discharge.")

    pdf.section_title("6. SAFETY REPORTING")
    pdf.body_text("Adverse events will be classified per VARC-3 (Valve Academic Research Consortium 3) definitions. All serious adverse events (SAEs) must be reported to the sponsor within 24 hours of awareness. Adverse events of special interest include: all-cause mortality, stroke (disabling and non-disabling), major bleeding, acute kidney injury (AKIN stage 2 or 3), major vascular complications, new permanent pacemaker implantation, and device-related complications (migration, embolization, paravalvular leak requiring reintervention).")

    if amendment_num:
        pdf.add_page()
        pdf.section_title(f"AMENDMENT {amendment_num} SUMMARY OF CHANGES")
        if amendment_num == "1":
            pdf.body_text(f"This amendment modifies the inclusion criteria to broaden the eligible patient population. Specifically, the minimum aortic valve area threshold has been revised and the age requirement has been updated based on interim data analysis.")
            pdf.bullet_list([
                "Section 4.1, Criterion 1: Age requirement changed from >= 21 years to >= 18 years",
                "Section 4.1, Criterion 2: AVA threshold changed from < 0.8 cm2 to < 1.0 cm2",
                "Section 5.1: Added CT fractional flow reserve (CT-FFR) as optional screening assessment",
                "Section 6: Updated SAE reporting timeline from 48 hours to 24 hours per regulatory feedback",
            ])
        elif amendment_num == "2":
            pdf.body_text("This amendment adds new secondary endpoints based on FDA feedback and updates the follow-up schedule.")
            pdf.bullet_list([
                "Section 2.2: Added NYHA functional class improvement at 1 year as secondary endpoint",
                "Section 2.2: Added new permanent pacemaker implantation rate at 30 days as secondary endpoint",
                "Section 3: Extended follow-up from 3 years to 5 years per FDA request",
                "Section 4.2: Added exclusion criterion for patients with estimated life expectancy < 24 months",
                "Section 5.3: Added mandatory 72-hour continuous ECG monitoring post-procedure",
            ])

    filename = f"{trial_key}_Protocol_{version.replace('.', '_')}"
    if amendment_num:
        filename += f"_Amendment_{amendment_num}"
    filepath = os.path.join(OUTPUT_DIR, f"{filename}.pdf")
    pdf.output(filepath)
    return filepath


def generate_icf(trial_key, trial, version, site_id, site_info):
    trial_display = trial_key.replace("_", " ")
    pdf = TrialPDF(trial_display, "Informed Consent Form", version)
    pdf.alias_nb_pages()
    add_disclaimer_page(pdf)
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(0, 51, 102)
    pdf.ln(10)
    pdf.cell(0, 10, "INFORMED CONSENT FORM", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(0, 8, f"The {trial_display} Clinical Trial", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(50, 50, 50)
    pdf.cell(0, 7, f"Site: {site_info['name']} ({site_id})", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, f"Principal Investigator: {site_info['pi']}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, f"Location: {site_info['city']}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, f"ICF Version: {version}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, f"Sponsor: {trial['sponsor']}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)

    pdf.section_title("INTRODUCTION")
    pdf.body_text(f"You are being invited to take part in a research study. This form provides important information about the study, including the purpose, what you will be asked to do, and the possible risks and benefits. Please read this document carefully and ask questions about anything you do not understand before deciding whether or not to participate.")
    pdf.body_text(f"The purpose of this study is to evaluate the safety and effectiveness of the {trial['device']} for the treatment of {trial['indication']}.")

    pdf.section_title("WHAT IS THE PURPOSE OF THIS STUDY?")
    pdf.body_text(f"{trial['population'].capitalize()}. Your doctor believes that you may be eligible for this study. The {trial['device']} is an investigational device, which means it has not been fully approved by the U.S. Food and Drug Administration (FDA) for the use being studied.")

    pdf.section_title("WHAT WILL HAPPEN IF I TAKE PART IN THIS STUDY?")
    pdf.body_text("If you agree to participate, you will undergo screening tests to determine your eligibility. If eligible, you will undergo the study procedure and be followed for up to 5 years. Follow-up visits will include physical examinations, echocardiography, blood tests, electrocardiograms, and quality of life questionnaires.")

    pdf.section_title("WHAT ARE THE RISKS?")
    pdf.body_text("As with any heart procedure, there are risks including but not limited to: stroke, bleeding, blood vessel damage, kidney injury, need for a permanent pacemaker, infection, and in rare cases, death. The device may also migrate or cause a paravalvular leak that requires additional procedures.")
    pdf.body_text("Additional risks specific to this procedure include damage to surrounding heart structures, hemolysis (destruction of red blood cells), and the possibility that the device may not function as intended, requiring surgical intervention.")

    pdf.section_title("WHAT ARE THE BENEFITS?")
    pdf.body_text(f"The potential benefits include improvement in symptoms related to {trial['indication']}, improved quality of life, and improved heart function. However, there is no guarantee that you will benefit from participation in this study.")

    pdf.section_title("ALTERNATIVES TO PARTICIPATION")
    pdf.body_text("You do not have to participate in this study. Alternative treatments may include continued medical management with medications, surgical valve repair or replacement, or other approved transcatheter procedures. Your doctor can discuss these options with you.")

    pdf.section_title("CONFIDENTIALITY")
    pdf.body_text("Your medical records and research data will be kept confidential to the extent permitted by law. The study sponsor, regulatory authorities (such as the FDA), and the Institutional Review Board (IRB) may review your records for quality assurance and regulatory compliance. Your name will not appear in any published results.")

    pdf.section_title("CONTACT INFORMATION")
    pdf.body_text(f"If you have questions about the study, contact the Principal Investigator, {site_info['pi']}, at {site_info['name']}, {site_info['city']}.")
    pdf.body_text("If you have questions about your rights as a research participant, contact the IRB at the number listed on the site-specific addendum.")

    pdf.section_title("CONSENT SIGNATURE")
    pdf.body_text("By signing below, I confirm that I have read this informed consent form, have had the opportunity to ask questions, and voluntarily agree to participate in this research study.")
    pdf.ln(5)
    pdf.body_text("_________________________________          ________________")
    pdf.body_text("Subject Signature                                         Date")
    pdf.ln(3)
    pdf.body_text("_________________________________          ________________")
    pdf.body_text("Printed Name of Subject                                Date")
    pdf.ln(3)
    pdf.body_text("_________________________________          ________________")
    pdf.body_text("Person Obtaining Consent                              Date")

    filepath = os.path.join(OUTPUT_DIR, f"{trial_key}_ICF_{version.replace('.', '_')}_{site_id}.pdf")
    pdf.output(filepath)
    return filepath


def generate_regulatory_submission(trial_key, trial, sub_type, jurisdiction):
    trial_display = trial_key.replace("_", " ")
    pdf = TrialPDF(trial_display, f"Regulatory Submission - {jurisdiction}", "v1.0")
    pdf.alias_nb_pages()
    add_disclaimer_page(pdf)
    pdf.add_page()
    pdf.cell(0, 8, f"{sub_type}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(0, 8, f"The {trial_display} Clinical Trial", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(50, 50, 50)
    pdf.cell(0, 7, f"Device: {trial['device']}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, f"Jurisdiction: {jurisdiction}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, f"Sponsor: {trial['sponsor']}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)

    pdf.section_title("1. EXECUTIVE SUMMARY")
    pdf.body_text(f"This document constitutes the {sub_type} for the {trial['device']}. The submission provides a comprehensive summary of the clinical evidence supporting the safety and effectiveness of the device for the treatment of {trial['indication']}.")

    if jurisdiction == "FDA":
        pdf.body_text("This Premarket Approval (PMA) supplement is submitted to the U.S. Food and Drug Administration under 21 CFR 814.39. The supplement includes updated clinical data from the pivotal trial, manufacturing process updates, and revised labeling.")
    elif jurisdiction == "CE Mark":
        pdf.body_text("This Clinical Evaluation Report (CER) is prepared in accordance with MEDDEV 2.7/1 Rev. 4 and EU MDR 2017/745 Article 61. The report summarizes the clinical data supporting the CE marking of the device for distribution in the European Economic Area.")
    elif jurisdiction == "Health Canada":
        pdf.body_text("This medical device licence application is submitted to Health Canada under the Medical Devices Regulations (SOR/98-282). The application includes the Clinical Investigation Summary, risk-benefit analysis, and evidence of compliance with applicable standards (ISO 13485, ISO 14971).")

    pdf.section_title("2. DEVICE DESCRIPTION")
    pdf.body_text(f"The {trial['device']} is designed for transcatheter delivery via the femoral vein or femoral artery approach. The device consists of a self-expanding nitinol frame with bovine pericardial tissue leaflets. The delivery system features a steerable catheter with a flush-port mechanism for intra-procedural hemodynamic monitoring.")

    pdf.section_title("3. CLINICAL EVIDENCE SUMMARY")
    pdf.body_text(f"The pivotal {trial_display} trial ({trial['nct']}) is a {trial['design']} study evaluating the {trial['device']} in {trial['population']}.")
    pdf.body_text(f"Primary Endpoint: {trial['primary_endpoint']}.")
    pdf.subsection_title("3.1 Enrollment Summary")
    pdf.body_text("As of the data cut-off date, a total of 350 subjects have been enrolled across 45 investigational sites in the United States, Europe, and Canada. Subject demographics: mean age 78.2 years, 52% female, mean STS-PROM score 5.8%.")
    pdf.subsection_title("3.2 Primary Endpoint Results")
    pdf.body_text("The primary endpoint was met. Details of the clinical outcomes are provided in the appended Clinical Study Report (CSR). The results demonstrated a clinically meaningful and statistically significant benefit relative to the performance goal derived from historical controls.")

    pdf.section_title("4. RISK-BENEFIT ANALYSIS")
    pdf.body_text(f"The risk-benefit profile of the {trial['device']} is favorable for the intended population. The observed rates of adverse events are consistent with or lower than those reported in comparable transcatheter valve therapies. The clinical benefits, including symptom improvement (NYHA class), quality of life (KCCQ), and hemodynamic performance (valve gradient, regurgitation grade), outweigh the identified risks.")

    filepath = os.path.join(OUTPUT_DIR, f"{trial_key}_Regulatory_{jurisdiction.replace(' ', '_')}.pdf")
    pdf.output(filepath)
    return filepath


def main():
    generated = []

    generated.append(generate_protocol("ALLIANCE", TRIALS["ALLIANCE"], "v3.0"))
    generated.append(generate_protocol("ALLIANCE", TRIALS["ALLIANCE"], "v4.0", amendment_num="1"))
    generated.append(generate_protocol("ALLIANCE", TRIALS["ALLIANCE"], "v5.0", amendment_num="2"))

    for site_id, site_info in TRIALS["ALLIANCE"]["sites"].items():
        generated.append(generate_icf("ALLIANCE", TRIALS["ALLIANCE"], "v2.0", site_id, site_info))
        generated.append(generate_icf("ALLIANCE", TRIALS["ALLIANCE"], "v3.1", site_id, site_info))

    generated.append(generate_protocol("ENCIRCLE", TRIALS["ENCIRCLE"], "v2.0"))
    generated.append(generate_protocol("ENCIRCLE", TRIALS["ENCIRCLE"], "v3.0", amendment_num="1"))

    for site_id, site_info in TRIALS["ENCIRCLE"]["sites"].items():
        generated.append(generate_icf("ENCIRCLE", TRIALS["ENCIRCLE"], "v1.0", site_id, site_info))

    generated.append(generate_regulatory_submission("ENCIRCLE", TRIALS["ENCIRCLE"], "PMA Supplement", "FDA"))

    generated.append(generate_protocol("TRISCEND_II", TRIALS["TRISCEND_II"], "v4.0", amendment_num="1"))
    generated.append(generate_protocol("TRISCEND_II", TRIALS["TRISCEND_II"], "v5.0", amendment_num="2"))

    for site_id, site_info in TRIALS["TRISCEND_II"]["sites"].items():
        generated.append(generate_icf("TRISCEND_II", TRIALS["TRISCEND_II"], "v3.0", site_id, site_info))

    generated.append(generate_regulatory_submission("TRISCEND_II", TRIALS["TRISCEND_II"], "Clinical Evaluation Report", "CE Mark"))
    generated.append(generate_regulatory_submission("TRISCEND_II", TRIALS["TRISCEND_II"], "Medical Device Licence Application", "Health Canada"))

    generated.append(generate_protocol("ALT_FLOW_II", TRIALS["ALT_FLOW_II"], "v1.0"))
    generated.append(generate_protocol("ALT_FLOW_II", TRIALS["ALT_FLOW_II"], "v2.0", amendment_num="1"))

    generated.append(generate_protocol("CLASP_IID", TRIALS["CLASP_IID"], "v3.0"))

    print(f"\nGenerated {len(generated)} PDFs:")
    for f in generated:
        print(f"  {os.path.basename(f)}")


if __name__ == "__main__":
    main()
