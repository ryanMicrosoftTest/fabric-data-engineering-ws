CREATE TABLE DoctorMemos (
    id INT IDENTITY(1,1) PRIMARY KEY,       -- Auto-incrementing ID
    doctor_name NVARCHAR(200) NOT NULL,     -- Doctor's name
    memo_date DATE NOT NULL,                -- Date of the memo
    memo NVARCHAR(MAX) NOT NULL             -- Full memo text (supports very long text)
);






DECLARE @memo NVARCHAR(MAX) = N'Memo 1 — General Medicine (Bronchitis Follow-up)
Date: 2025-09-22
Hospital: Contoso General Hospital (Outpatient Clinic)
Doctor: Dr. Helena Park, MD — License: MA-MED-457821; Email: h.park@contosohealth.example; Fax: (555) 0199-2233
Patient: Jonathan “Jon” Hale — Age: 43 — Profession: Elementary school teacher — Organization: Fabrikam Public Schools
Address: 123 Harbor St, City: Boston, State: MA, Zip: 02110, CountryOrRegion: United States
Phone: (555) 0145-8876 — Email: jon.hale@example.com — Username: jhale82
MedicalRecord: MRN M-2025-00981 — HealthPlan: BluePlus Gold HMO ID BP-9922-4433 — Account: BA-0045123987
Government/Other IDs: SocialSecurity: 123-45-6789; IDNum (Passport): USA X12345678; License (Driver): MA D12345678
Telehealth metadata: IPAddress: 203.0.113.45 — Url (portal): https://portal.examplehealth.org/record/M-2025-00981
Devices: Home nebulizer Device SN-NB-77F9-221; Apple Watch Device S/N AW-6FQ2-9911
Biometric: BioID retinal scan ID RS-9982 captured for secure sign-in
Vehicle: 2013 Honda Civic — VIN: 1HGCM82633A004352 — Plate: MA 7KJ-P23
LocationOther: Boston Common (patient reports brisk walking here daily)
Unknown: Internal Note Token: XQ-9011-UNCL
Assessment/Plan: Persistent cough improving; CXR consistent with bronchitis. Continue albuterol via nebulizer SN-NB-77F9-221, azithromycin day 3/5. School exposure likely. Return if fever > 101.5°F or dyspnea.';



INSERT INTO DoctorMemos (doctor_name, memo_date, memo)
VALUES (
    N'Dr. Helena Park, MD',
    '2025-09-22',
	@memo
)


DECLARE @memo VARCHAR(MAX) = N'Memo 2 — Cardiology (Hypertension & Palpitations)
Date: 2025-08-30
Hospital: North Seaport Heart & Vascular Institute
Doctor: Dr. Omar Reyes, DO — License: MA-DO-883210 — Email: o.reyes@nsheart.example — Fax: (555) 0166-7700
Patient: Maria Lopez — Age: 50 — Profession: Accountant — Organization: Contoso Logistics, Inc.
Address: 44 Seaport Blvd, City: Boston, State: MA, Zip: 02210, CountryOrRegion: United States
Phone: (555) 0177-2210 — Email: maria.lopez@example.com — Username: maria.lopez78
MedicalRecord: MRN C-554-8892 — HealthPlan: Fabrikam PPO ID FP-3344-2200 — Account: A-881102-77
IDs: SocialSecurity: 987-65-4321; IDNum (Passport): USA Y98765432; License (Driver): MA L99887766
Connectivity: IPAddress: 198.51.100.72 — Url: https://cardio.examplehealth.org/patients/C-554-8892
Devices: Holter monitor Device HM-24 S/N 24H-5521; BP cuff Device BC-10 S/N BC10-0033
BioID: Fingerprint template BioID FP-4411 registered for device pickup
Vehicle: Subaru Outback — VIN: 4S4BSENC0H3322110 — Plate: MA 5FT-9Z2
LocationOther: Logan Airport, Terminal B (reported palpitations while traveling)
Unknown: NoteRef UQ-22A-ALPHA
Assessment/Plan: Palpitations likely PACs; Holter placed. Increase lisinopril to 20 mg daily; lifestyle counseling. ER precautions reviewed.';

INSERT INTO DoctorMemos (doctor_name, memo_date, memo)
VALUES(
	N'Dr. Omar Reyes, DO',
	'2025-08-30',
	@memo
);


DECLARE @memo VARCHAR(MAX) = N'Memo 3 — Endocrinology (Type 1 Diabetes Titration)
Date: 2025-09-10
Hospital: Fabrikam Medical Center — Endocrinology
Doctor: Dr. Priya Nandakumar, MD — License: MA-MED-778901 — Email: p.nandakumar@endocrine.example — Fax: (555) 0198-4411
Patient: David Chen — Age: 35 — Profession: Software engineer — Organization: Tailwind Apps, LLC
Address: 9 Beacon Hill Rd, City: Boston, State: MA, Zip: 02108, CountryOrRegion: United States
Phone: (555) 0102-9090 — Email: d.chen@example.com — Username: dchen_dev
MedicalRecord: MRN E-10029 — HealthPlan: Aetna Choice POS II ID AC-9911-5533 — Account: ACC-2200-0081
IDs: SocialSecurity: 321-54-9876; IDNum (Passport): USA P44556677; License (Driver): MA D44556677
Tech: Televisit IPAddress: 192.0.2.56 — Url: https://endo.examplehealth.org/chart/E-10029
Devices: Device Dexcom G6 SN G6-552-118; Insulin pump Device Tandem t:slim S/N TS-771-993
BioID: BioID voiceprint VP-3301 enrolled for pump support line
Vehicle: Toyota Prius — VIN: JTDKB20U693511222 — Plate: MA ECO-992
LocationOther: Charles River Esplanade (pre-meal run pattern)
Unknown: CaseToken NX-77Q
Assessment/Plan: A1c 8.2%. Adjusted basal rates; reviewed carb ratios. Hypoglycemia protocol reinforced.';

INSERT INTO DoctorMemos(doctor_name, memo_date, memo)
VALUES(
	N'Dr. Priya Nandakumar, MD',
	'2025-09-10',
	@memo
);


DECLARE @memo VARCHAR(MAX) = N'Memo 4 — Orthopedics (ACL Tear Rehab)
Date: 2025-07-25
Hospital: Seaside Orthopedic Hospital
Doctor: Dr. Hannah Kim, MD — License: MA-ORTHO-662341 — Email: h.kim@seasideortho.example — Fax: (555) 0133-6442
Patient: Robert Wilson — Age: 54 — Profession: Sales manager — Organization: Fabrikam Aerospace
Address: 77 Atlantic Ave, City: Boston, State: MA, Zip: 02111, CountryOrRegion: United States
Phone: (555) 0188-1122 — Email: robert.wilson@example.com — Username: r_wilson54
MedicalRecord: MRN OR-22019 — HealthPlan: Contoso HealthPlus ID CHP-2201-77 — Account: BIL-77110022
IDs: SocialSecurity: 246-80-1357; IDNum (Passport): USA N11223344; License (Driver): MA WIL-662034
Network: IPAddress: 203.0.113.199 — Url: https://ortho.examplehealth.org/OR-22019
Devices: Hinged knee brace Device KB-SN-99321; Home TENS Device TNS-11 SN T11-0099
BioID: BioID retinal scan RS-7710 at imaging desk
Vehicle: Ford F-150 — VIN: 1FTFW1EF1EKD12345 — Plate: MA TRK-441
LocationOther: Fenway Park (injury occurred ascending stairs)
Unknown: RefCode Z-PL-004
Assessment/Plan: Partial ACL tear; PT 2x/week x 6 weeks, brace use reviewed; consider surgical consult if instability persists.';

INSERT INTO DoctorMemos (doctor_name, memo_date, memo)
VALUES(
	'Dr. Hannah Kim, MD',
	'2025-07-25',
	@memo
);


DECLARE @memo VARCHAR(MAX) = N'Memo 5 — OB/GYN (Gestational Diabetes Management)
Date: 2025-06-18
Hospital: Harbor Women’s Health
Doctor: Dr. Aisha Rahman, MD — License: MA-OB-559012 — Email: a.rahman@harborwh.example — Fax: (555) 0191-3355
Patient: Sarah Thompson — Age: 37 — Profession: Project manager — Organization: Northwind Traders
Address: 12 Seaglass Ct, City: Salem, State: MA, Zip: 01970, CountryOrRegion: United States
Phone: (555) 0150-6600 — Email: sarah.thompson@example.com — Username: sarah_t37
MedicalRecord: MRN OB-99102 — HealthPlan: BluePlus Maternity ID BPM-7788-4411 — Account: MAT-ACC-00192
IDs: SocialSecurity: 555-66-7777; IDNum (Passport): USA R22334455; License (Driver): MA T-11220990
Connectivity: IPAddress: 198.51.100.14 — Url: https://ob.examplehealth.org/OB-99102
Devices: Glucose meter Device GM-Alpha SN GM-A-8842; CGM Device Libre 3 SN L3-552-20
BioID: BioID fingerprint FP-8821 for lab kiosk
Vehicle: Volvo XC60 — VIN: YV4A22PK5L1123456 — Plate: MA MOM-220
LocationOther: Salem Willows Park (postprandial walks)
Unknown: Tag UNK-OB-52
Assessment/Plan: GDMA2; continue insulin, dietician referral, weekly NST at 32 weeks.';

INSERT INTO DoctorMemos(doctor_name, memo_date, memo)
VALUES(
	'Dr. Aisha Rahman, MD',
	'2025-06-18',
	@memo
);



DECLARE @memo VARCHAR(MAX) = N'Memo 6 — Psychiatry (GAD & Sleep Hygiene)
Date: 2025-09-05
Hospital: Evergreen Behavioral Health
Doctor: Dr. Leonard Wu, MD — License: MA-PSY-334455 — Email: l.wu@evergreenbh.example — Fax: (555) 0181-7755
Patient: Emily Davis — Age: 27 — Profession: Graphic designer — Organization: WideWorld Importers
Address: 200 Elm St, City: Cambridge, State: MA, Zip: 02139, CountryOrRegion: United States
Phone: (555) 0170-4419 — Email: emily.davis@example.com — Username: emdavis_design
MedicalRecord: MRN BH-33021 — HealthPlan: Contoso Behavioral ID CB-6611-9955 — Account: BHA-00931
IDs: SocialSecurity: 760-11-2468; IDNum (Passport): USA L55667788; License (Driver): MA E-88442277
Net/Portal: IPAddress: 192.0.2.88 — Url: https://behavioral.examplehealth.org/BH-33021
Devices: Sleep tracker Device Ring SN SR-221-991; Phone Device Android SN A50-88231
BioID: BioID FaceID template FID-2209 on patient device (not stored by clinic)
Vehicle: Volkswagen Golf — VIN: WVWZZZ1KZ6W123456 — Plate: MA CALM-7
LocationOther: Cambridge Common (mindfulness walks)
Unknown: NoteKey Q-PRIME-006
Assessment/Plan: Initiated sertraline 50 mg daily; CBT-I referral; reduce caffeine; safety plan reviewed.';

INSERT INTO DoctorMemos(doctor_name, memo_date, memo)
VALUES(
	'Dr. Leonard Wu, MD',
	'2025-09-05',
	@memo
);


DECLARE @memo VARCHAR(MAX) = N'Memo 7 — Gastroenterology (IBS Flare)
Date: 2025-08-12
Hospital: Riverbend Digestive Health Center
Doctor: Dr. Elena García, MD — License: MA-GI-120045 — Email: e.garcia@riverbendgi.example — Fax: (555) 0175-9922
Patient: James Anderson — Age: 32 — Profession: Warehouse supervisor — Organization: Adventure Works
Address: 890 Riverside Dr, City: Lowell, State: MA, Zip: 01850, CountryOrRegion: United States
Phone: (555) 0160-2288 — Email: james.anderson@example.com — Username: j.anderson32
MedicalRecord: MRN GI-77720 — HealthPlan: HealthCo Silver ID HC-2211-7788 — Account: RB-ACCT-4477
IDs: SocialSecurity: 432-10-9876; IDNum (Passport): USA S99887766; License (Driver): MA A55443322
Network: IPAddress: 203.0.113.78 — Url: https://gi.examplehealth.org/GI-77720
Devices: Home BP monitor Device BP-SN-10101; Smart scale Device SS-200 SN SS-200-771
BioID: BioID palm vein template PV-9920 (building access)
Vehicle: Chevy Malibu — VIN: 1G1ZB5ST0JF123456 — Plate: MA RB-2201
LocationOther: Tsongas Center (symptoms worsened after event)
Unknown: GI-UNK-H7
Assessment/Plan: IBS-D flare; low-FODMAP diet guidance; trial rifaximin; stool studies ordered.';

INSERT INTO DoctorMemos(doctor_name, memo_date, memo)
VALUES(
	'Dr. Elena García, MD',
	'2025-08-12',
	@memo
);


DECLARE @memo VARCHAR(MAX) = N'Memo 8 — Pulmonology (Post-COVID Dyspnea)
Date: 2025-09-01
Hospital: Lakeside Pulmonary Clinic
Doctor: Dr. Naomi Patel, MD — License: MA-PULM-778812 — Email: n.patel@lakesidepulm.example — Fax: (555) 0197-3300
Patient: Michael Johnson — Age: 60 — Profession: Retired electrician — Organization: (former) City of Boston Utilities
Address: 15 Cedar Ln, City: Quincy, State: MA, Zip: 02169, CountryOrRegion: United States
Phone: (555) 0180-5500 — Email: michael.johnson@example.com — Username: mj_retired60
MedicalRecord: MRN P-80911 — HealthPlan: Medicare Advantage ID MA-2299-6644 — Account: LS-AC-992211
IDs: SocialSecurity: 101-22-3033; IDNum (Passport): USA T66778899; License (Driver): MA JN-44332211
Connectivity: IPAddress: 198.51.100.205 — Url: https://pulm.examplehealth.org/P-80911
Devices: Incentive spirometer Device IS-77 SN 77-IS-0042; Home oximeter Device OX-12 SN OX12-9022
BioID: BioID iris code IC-5501 (research study enrollment)
Vehicle: Ford Escape — VIN: 1FMCU0GD2HUE12345 — Plate: MA BREATHE
LocationOther: Castle Island (dyspnea on exertion noted)
Unknown: Token K-NULL-42
Assessment/Plan: PFTs show mild restriction; pulmonary rehab recommended; continue inhaled budesonide-formoterol.';


INSERT INTO DoctorMemos(doctor_name, memo_date, memo)
VALUES(
	'Dr. Naomi Patel, MD',
	'2025-09-01',
	@memo
);



DECLARE @memo VARCHAR(MAX) = N'Memo 9 — Neurology (Migraine Prophylaxis)
Date: 2025-08-05
Hospital: Summit Neurology Group
Doctor: Dr. Caroline Brooks, MD — License: MA-NEURO-991122 — Email: c.brooks@summitneuro.example — Fax: (555) 0144-8800
Patient: Olivia Martinez — Age: 40 — Profession: Marketing director — Organization: Fourth Coffee
Address: 300 Summit Way, City: Somerville, State: MA, Zip: 02143, CountryOrRegion: United States
Phone: (555) 0130-4412 — Email: olivia.martinez@example.com — Username: oliva_mktg
MedicalRecord: MRN N-112200 — HealthPlan: Alpine HSA ID AH-5533-9911 — Account: SUM-ACCT-5520
IDs: SocialSecurity: 654-32-1098; IDNum (Passport): USA U33445566; License (Driver): MA MZ-11223344
Connectivity: IPAddress: 192.0.2.190 — Url: https://neuro.examplehealth.org/N-112200
Devices: Smart headache diary Device HD-1 SN HD1-7788; Nightguard Device NG-22 SN NG-2281
BioID: BioID voiceprint VP-7741 used for telephone refills
Vehicle: Tesla Model 3 — VIN: 5YJ3E1EA7JF012345 — Plate: MA NEURA-1
LocationOther: Rose Kennedy Greenway (photosensitivity triggers noted)
Unknown: MiscRef NEU-X1
Assessment/Plan: Start propranolol 40 mg BID; riboflavin and magnesium supplements; follow-up in 6 weeks.';

INSERT INTO DoctorMemos(doctor_name, memo_date, memo)
VALUES(
	'Dr. Caroline Brooks, MD',
	'2025-08-05',
	@memo
);


DECLARE @memo VARCHAR(MAX) = N'Memo 10 — General Surgery (Appendectomy Post‑Op)
Date: 2025-09-03
Hospital: Bayview Surgical Center
Doctor: Dr. Samuel O’Neill, MD — License: MA-SURG-443321 — Email: s.oneill@bayviewsurg.example — Fax: (555) 0195-6677
Patient: Sophia Brown — Age: 24 — Profession: Barista — Organization: Woodgrove Coffee Roasters
Address: 88 Pine St, City: Worcester, State: MA, Zip: 01608, CountryOrRegion: United States
Phone: (555) 0122-7781 — Email: sophia.brown@example.com — Username: soph_b24
MedicalRecord: MRN GS-22002 — HealthPlan: Woodgrove HMO ID WH-2200-5533 — Account: BV-ACC-22190
IDs: SocialSecurity: 909-88-7766; IDNum (Passport): USA V22113344; License (Driver): MA SB-77665544
Net/Portal: IPAddress: 203.0.113.12 — Url: https://surgery.examplehealth.org/GS-22002
Devices: Wound vac Device WV-10 SN WV-10-441; Home thermometer Device HT-2 SN HT2-9920
BioID: BioID fingerprint FP-7712 for medication locker access
Vehicle: Honda Fit — VIN: JHMGD38458S123456 — Plate: MA MINI-05
LocationOther: Elm Park (walking tolerated 20 minutes)
Unknown: SURG-UNK-9B
Assessment/Plan: Incisions clean/dry/intact; afebrile; continue oral antibiotics; avoid heavy lifting for 2 weeks; return precautions reviewed.';

INSERT INTO DoctorMemos(doctor_name, memo_date, memo)
VALUES(
	'Dr. Samuel O’Neill, MD',
	'2025-09-03',
	@memo
);