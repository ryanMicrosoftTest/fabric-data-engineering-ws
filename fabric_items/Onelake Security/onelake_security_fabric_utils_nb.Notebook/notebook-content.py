# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "5310979b-49b7-82b4-4033-b21c0e4be517",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import fabric_utils
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from fabric_utils import onelake_roles

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id_secret = 'fuam-spn-client-id'
tenant_id_secret = 'fuam-spn-tenant-id'
client_secret_name = 'fuam-spn-secret'

workspace_id = 'a8cbda3d-903e-4154-97d9-9a91c95abb42'
lakehouse_id = '25ae3fb0-1c96-49eb-8a0f-208c350dc4ba'

tenant_id = '35acf02c-4b87-4ae6-9221-ff5cafd430b4'

# all these users must have Viewer role on Workspace
object_id_user_neuro_role = '662b14b6-a936-46b0-bde0-9f4d759dc46e'      # dr klerx
object_id_for_gastro_role = '6338eaf2-5a87-4eb3-b23b-ea30d04de02a'      # dr muir
object_id_for_ortho_role = 'b00395d7-230b-41bb-8bbc-d668979bd8f6'       # purview admin

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_api_token_via_akv(kv_uri:str, client_id_secret:str, tenant_id_secret:str, client_secret_name:str)->str:
    """
    Function to retrieve an api token used to authenticate with Microsoft Fabric APIs

    kv_uri:str: The uri of the azure key vault
    client_id_secret:str: The name of the key used to store the value for the client id in the akv
    tenant_id_secret:str: The name of the key used to store the value for the tenant id in the akv
    client_secret_name:str: The name of the key used to store the value for the client secret in the akv

    """
    client_id = notebookutils.credentials.getSecret(kv_uri, client_id_secret)
    tenant_id = notebookutils.credentials.getSecret(kv_uri, tenant_id_secret)
    client_secret = notebookutils.credentials.getSecret(kv_uri, client_secret_name)

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    scope = 'https://analysis.windows.net/powerbi/api/.default'
    token = credential.get_token(scope).token

    return token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get oauth token
token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create Table

# CELL ********************

doctor_list = [
    'Dr. Stephany Quixada, MD',
    'Dr. Stephany Quixada, MD',
    'Dr. Stephany Quixada, MD',
    'Dr. Stephany Quixada, MD',
    'Dr. Jean Urbonas, MD',
    'Dr. Jean Urbonas, MD',
    'Dr. Jean Urbonas, MD',
    'Dr. Jean Urbonas, MD',
    'Dr. Wesson Arkwright, MD',
    'Dr. Wesson Arkwright, MD',
    'Dr. Wesson Arkwright, MD',
    'Dr. Trista Beltran, MD',
    'Dr. Trista Beltran, MD',
    'Dr. Trista Beltran, MD',
    'Dr. Perla Swartz, MD',
    'Dr. Perla Swartz, MD',
    'Dr. Perla Swartz, MD',
    'Dr. Perla Swartz, MD',
    'Dr. Monroe Koster, MD',
    'Dr. Monroe Koster, MD',
    'Dr. Monroe Koster, MD',
    'Dr. Monroe Koster, MD',
    'Dr. Kwame M’Meeuwis, MD',
    'Dr. Kwame M’Meeuwis, MD',
    'Dr. Kwame M’Meeuwis, MD',
    'Dr. Kwame M’Meeuwis, MD',
    'Dr. Antionette Klerkx, MD',
    'Dr. Antionette Klerkx, MD',
    'Dr. Antionette Klerkx, MD',
    'Dr. Antionette Klerkx, MD',
    'Dr. Harley Muir, MD',
    'Dr. Harley Muir, MD',
    'Dr. Harley Muir, MD',
    'Dr. Harley Muir, MD'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

memo_list = [
'Memo 8 — Pulmonology (Post-COVID Dyspnea)↵Date: 2025-08-26↵Hospital: Augusta Medical Center↵Doctor: Dr. Stephany Quixada, MD — License: FO-CWWU-609506 — Email: stupendouspolarbear87@aol.com — Fax: (108) 1405-2843↵Patient: Boden Yamagishi — Age: 60 — Profession: Retired air traffic controller Organization: (former) PayPal Vanderbilt University Hospital↵Address: 48 Stow Ter, City: Gila, State: SD, Zip: 74562, CountryOrRegion: United States↵Phone: (108) 4834-6644 — Email: stupendousmovie221@live.com — Username: ri_iqcmdbl19↵MedicalRecord: MRN C-75720 — HealthPlan: Freedom Health ID PS-7100-0638 — Account: DY-FO-522430↵IDs: SocialSecurity: 209-77-9470; IDNum (Passport): MetroPlus Health U36608066; License (Driver): SU IO-59553977↵Connectivity: IPAddress: 198.51.100.205 — Url: https://lpfk.pdfswtnywcpk.org/C-75720↵Devices: Incentive spirometer Device IS-96 WU 73-DR-2855; Home oximeter Device BN-72 SN SK57-5860↵BioID: BioID iris code MI-3243 (research study enrollment)↵Vehicle: Lava Cast Forest — VIN: 6OEEH8AI0WAS76856 — Plate: DWORSHAK DAM↵LocationOther: Lake Clark (dyspnea on exertion noted)↵Unknown: Token H-UQXJ-97↵Assessment/Plan: PFTs show mild restriction; pulmonary rehab recommended; continue inhaled budesonide-formoterol.',
'Memo 16 — Gastroenterology (GERD Management)↵Date: 2025-09-22↵Hospital: Keck Hospital (Outpatient Clinic)↵Doctor: Dr. Stephany Quixada, MD — License: QY-APC-829189; Email: drstephanyquixada@hospital.example; Fax: (108) 5435-9606↵Patient: Iris “Iris” Ahmed — Age: 49 — Profession: Developer Organization: Seaside Market↵Address: 939 Oak Blvd, City: Boulder, State: CO, Zip: 61877, CountryOrRegion: United States↵Phone: (108) 99606-8606 — Email: iris.ahmed@example.com — Username: iahmed46↵MedicalRecord: MRN W-1699-47306 — HealthPlan: Bright Health HMO ID NN-4345-4476 — Account: NM-7669627106↵Government/Other IDs: SocialSecurity: 307-61-2566; IDNum (Passport): MetroPlus Health W31789906; License (Driver): SU N79623806↵Telehealth metadata: IPAddress: 203.0.113.106 — Url (portal): https://portal16.example.org/ejwffz/T-4356-48000↵Devices: Home nebulizer Device OB-ZC-97B8-986; Apple Watch Device S/P DH-7ZI9-0056↵Biometric: BioID retinal scan ID ZV-2332 captured for secure sign-in↵Vehicle: 2016 Horizon Academy — VIN: 9GQRT13349I760906 — Plate: SU 3ZM-J306↵LocationOther: Riverside Trail (evening strolls)↵Unknown: Internal Note Token: YN-5856-UNCL↵Assessment/Plan: Symptoms of GERD improved; continue PPI; lifestyle counseling provided.',
'Memo 25 — Orthopedics (Knee Pain Evaluation)↵Date: 2025-10-01↵Hospital: St. Raphael Clinic (Outpatient Clinic)↵Doctor: Dr. Stephany Quixada, MD — License: QY-APC-829189; Email: drstephanyquixada@hospital.example; Fax: (108) 5435-9606↵Patient: Marcus “Marcus” Klein — Age: 46 — Profession: Manager Organization: Fabrikam Foods↵Address: 948 Willow Rd, City: Salem, State: OR, Zip: 61886, CountryOrRegion: United States↵Phone: (108) 99615-8615 — Email: marcus.klein@example.com — Username: mklein55↵MedicalRecord: MRN W-1699-47315 — HealthPlan: Bright Health HMO ID NN-4345-4485 — Account: NM-7669627115↵Government/Other IDs: SocialSecurity: 307-61-2575; IDNum (Passport): MetroPlus Health W31789915; License (Driver): SU N79623815↵Telehealth metadata: IPAddress: 203.0.113.115 — Url (portal): https://portal25.example.org/ejwffz/T-4356-48009↵Devices: Home nebulizer Device OB-ZC-97B8-995; Apple Watch Device S/P DH-7ZI9-0065↵Biometric: BioID retinal scan ID ZV-2341 captured for secure sign-in↵Vehicle: 2010 Horizon Academy — VIN: 9GQRT13349I760915 — Plate: SU 3ZM-J315↵LocationOther: Downtown Loop (commuter walking)↵Unknown: Internal Note Token: YN-58515-UNCL↵Assessment/Plan: Knee pain likely overuse; RICE, PT referral; NSAIDs PRN; imaging if persists.',
'Memo 34 — General Medicine (Bronchitis Follow-up)↵Date: 2025-10-10↵Hospital: Keck Hospital (Outpatient Clinic)↵Doctor: Dr. Stephany Quixada, MD — License: QY-APC-829189; Email: drstephanyquixada@hospital.example; Fax: (108) 5435-9606↵Patient: Elena “Elena” Santos — Age: 43 — Profession: Engineer Organization: Seaside Market↵Address: 957 Holladay Ln, City: Madison, State: WI, Zip: 61895, CountryOrRegion: United States↵Phone: (108) 99624-8624 — Email: elena.santos@example.com — Username: esantos64↵MedicalRecord: MRN W-1699-47324 — HealthPlan: Bright Health HMO ID NN-4345-4494 — Account: NM-7669627124↵Government/Other IDs: SocialSecurity: 307-61-2584; IDNum (Passport): MetroPlus Health W31789924; License (Driver): SU N79623824↵Telehealth metadata: IPAddress: 203.0.113.124 — Url (portal): https://portal34.example.org/ejwffz/T-4356-48018↵Devices: Home nebulizer Device OB-ZC-97B8-9104; Apple Watch Device S/P DH-7ZI9-0074↵Biometric: BioID retinal scan ID ZV-2350 captured for secure sign-in↵Vehicle: 2019 Horizon Academy — VIN: 9GQRT13349I760924 — Plate: SU 3ZM-J324↵LocationOther: McKenzie Pass (patient reports brisk walking here daily)↵Unknown: Internal Note Token: YN-58524-UNCL↵Assessment/Plan: Persistent cough improving; CXR consistent with bronchitis. Continue albuterol via nebulizer, azithromycin day 5/5.',
'Memo 5 — OB/GYN (Gestational Diabetes Management)↵Date: 2025-06-12↵Hospital: Community Memorial Hospital Health↵Doctor: Dr. Jean Urbonas, MD — License: IO-ZX-349335 — Email: k.nbfgmi@pkmnasga.example — Fax: (108) 1342-1317↵Patient: Suzette Muller — Age: 37 — Profession: Plant Driver Organization: Dewdrop Drinks↵Address: 70 Cecilia Cir, City: Dollar Point, State: SD, Zip: 46546, CountryOrRegion: United States↵Phone: (108) 7661-7903 — Email: xsmgv.jhjgfeox@aiogiqx.hxe — Username: stupendoustiger926_k11↵MedicalRecord: MNO GT-40758 — HealthPlan: UnitedHealth Maternity ID TIB-8980-9590 — Account: ZFO-NOG-61471↵IDs: SocialSecurity: 085-30-8436; IDNum (Passport): MetroPlus Health C43024052; License (Driver): SU T-85378882↵Connectivity: IPAddress: 198.51.100.14 — Url: https://yd.pdfswtnywcpk.org/GT-40758↵Devices: Glucose meter Device GM-Alpha SN GM-E-4420; CGM Device Libre 3 SN L3-015-37↵BioID: BioID fingerprint AC-2479 for lab kiosk↵Vehicle: Volvo GY83 — VIN: VR8J72AE0M0178855 — Plate: Chandler Regional Medical Center SANTA CATALINA ISLAND-220↵LocationOther: Summit Lake (postprandial prospect peak)↵Unknown: Harley MFW-XL-92↵Assessment/Plan: NWMS2; continue insulin, dietician referral, weekly NST at 32 weeks.',
'Memo 12 — Endocrinology (Diabetes Management)↵Date: 2025-09-18↵Hospital: Mercy General Hospital (Outpatient Clinic)↵Doctor: Dr. Jean Urbonas, MD — License: QY-APC-829189; Email: drjeanurbonas@hospital.example; Fax: (108) 5435-9606↵Patient: Samuel “Samuel” Ortiz — Age: 45 — Profession: Nurse Organization: Contoso Builders↵Address: 935 Pine St, City: Dover, State: DE, Zip: 61873, CountryOrRegion: United States↵Phone: (108) 99602-8602 — Email: samuel.ortiz@example.com — Username: sortiz42↵MedicalRecord: MRN W-1699-47302 — HealthPlan: Bright Health HMO ID NN-4345-4472 — Account: NM-7669627102↵Government/Other IDs: SocialSecurity: 307-61-2562; IDNum (Passport): MetroPlus Health W31789902; License (Driver): SU N79623802↵Telehealth metadata: IPAddress: 203.0.113.102 — Url (portal): https://portal12.example.org/ejwffz/T-4356-47996↵Devices: Home nebulizer Device OB-ZC-97B8-982; Apple Watch Device S/P DH-7ZI9-0052↵Biometric: BioID retinal scan ID ZV-2328 captured for secure sign-in↵Vehicle: 2012 Horizon Academy — VIN: 9GQRT13349I760902 — Plate: SU 3ZM-J302↵LocationOther: Riverside Trail (evening strolls)↵Unknown: Internal Note Token: YN-5852-UNCL↵Assessment/Plan: A1c stable; continue metformin; reinforce diet & exercise; CGM review next visit.',
'Memo 21 — Pulmonology (Asthma Follow-up)↵Date: 2025-09-27↵Hospital: Riverside Medical Pavilion (Outpatient Clinic)↵Doctor: Dr. Jean Urbonas, MD — License: QY-APC-829189; Email: drjeanurbonas@hospital.example; Fax: (108) 5435-9606↵Patient: Avery “Avery” Chen — Age: 54 — Profession: Analyst Organization: Tailspin Toys↵Address: 944 Elm Dr, City: Burlingame, State: CA, Zip: 61882, CountryOrRegion: United States↵Phone: (108) 99611-8611 — Email: avery.chen@example.com — Username: achen51↵MedicalRecord: MRN W-1699-47311 — HealthPlan: Bright Health HMO ID NN-4345-4481 — Account: NM-7669627111↵Government/Other IDs: SocialSecurity: 307-61-2571; IDNum (Passport): MetroPlus Health W31789911; License (Driver): SU N79623811↵Telehealth metadata: IPAddress: 203.0.113.111 — Url (portal): https://portal21.example.org/ejwffz/T-4356-48005↵Devices: Home nebulizer Device OB-ZC-97B8-991; Apple Watch Device S/P DH-7ZI9-0061↵Biometric: BioID retinal scan ID ZV-2337 captured for secure sign-in↵Vehicle: 2021 Horizon Academy — VIN: 9GQRT13349I760911 — Plate: SU 3ZM-J311↵LocationOther: Downtown Loop (commuter walking)↵Unknown: Internal Note Token: YN-58511-UNCL↵Assessment/Plan: Asthma stable on current regimen; refill inhaler; spacer technique reviewed.',
'Memo 30 — Dermatology (Rash Evaluation)↵Date: 2025-10-06↵Hospital: Mercy General Hospital (Outpatient Clinic)↵Doctor: Dr. Jean Urbonas, MD — License: QY-APC-829189; Email: drjeanurbonas@hospital.example; Fax: (108) 5435-9606↵Patient: Willard “Willard” Nardi — Age: 51 — Profession: Designer Organization: Contoso Builders↵Address: 953 Cedar Ct, City: Wonder Lake, State: SD, Zip: 61891, CountryOrRegion: United States↵Phone: (108) 99620-8620 — Email: willard.nardi@example.com — Username: wnardi60↵MedicalRecord: MRN W-1699-47320 — HealthPlan: Bright Health HMO ID NN-4345-4490 — Account: NM-7669627120↵Government/Other IDs: SocialSecurity: 307-61-2580; IDNum (Passport): MetroPlus Health W31789920; License (Driver): SU N79623820↵Telehealth metadata: IPAddress: 203.0.113.120 — Url (portal): https://portal30.example.org/ejwffz/T-4356-48014↵Devices: Home nebulizer Device OB-ZC-97B8-9100; Apple Watch Device S/P DH-7ZI9-0070↵Biometric: BioID retinal scan ID ZV-2346 captured for secure sign-in↵Vehicle: 2015 Horizon Academy — VIN: 9GQRT13349I760920 — Plate: SU 3ZM-J320↵LocationOther: McKenzie Pass (patient reports brisk walking here daily)↵Unknown: Internal Note Token: YN-58520-UNCL↵Assessment/Plan: Topical steroid started for rash; avoid irritants; recheck in 10 days.',
'Memo 6 — Psychiatry (GAD & Sleep Hygiene)↵Date: 2025-08-30↵Hospital: VAMC↵Doctor: Dr. Wesson Arkwright, MD — License: LK-HMO-686580 — Email: x.xg@imwobcxfrgp.example — Fax: (108) 5965-8587↵Patient: Abigail Hampson — Age: 27 — Profession: Materials Manager Organization: Fairview Market↵Address: 211 Hilton Ln, City: Moriarty, State: SD, Zip: 81102, CountryOrRegion: United States↵Phone: (108) 6795-7735 — Email: stupendousdancer827@live.com — Username: emdavis_design↵MedicalRecord: MRN JD-74399 — HealthPlan: Contoso Behavioral ID BP-8540-5762 — Account: YQW-85907↵IDs: SocialSecurity: 886-08-0868; IDNum (Passport): MetroPlus Health J31525878; License (Driver): Oscar Health D-42149998↵Net/Portal: IPAddress: 192.0.2.88 — Url: https://qaallzctph.irjymcroxasfz.org/JD-74399↵Devices: Sleep tracker Device Ring SN RF-797-035; Phone Device Android SN B96-49853↵BioID: BioID FaceID template FID-2209 on patient device (not stored by clinic)↵Vehicle: Volkswagen Golf — VIN: YWNEJW0OP6P059415 — Plate: STUPENDOUSSHARK246 STUPENDOUSGUINEAPIG573-manhattan↵LocationOther: Balboa Terrace (mindfulness prospect peak)↵Unknown: NoteKey M-KFIWN-815↵Assessment/Plan: Initiated sertraline 50 mg daily; CBT-I referral; reduce caffeine; safety plan reviewed.',
'Memo 18 — General Medicine (Bronchitis Follow-up)↵Date: 2025-09-24↵Hospital: Mercy General Hospital (Outpatient Clinic)↵Doctor: Dr. Wesson Arkwright, MD — License: QY-APC-829189; Email: drwessonarkwright@hospital.example; Fax: (108) 5435-9606↵Patient: Priya “Priya” Garcia — Age: 51 — Profession: Engineer Organization: Contoso Builders↵Address: 941 Holladay Ln, City: Reno, State: NV, Zip: 61879, CountryOrRegion: United States↵Phone: (108) 99608-8608 — Email: priya.garcia@example.com — Username: pgarcia48↵MedicalRecord: MRN W-1699-47308 — HealthPlan: Bright Health HMO ID NN-4345-4478 — Account: NM-7669627108↵Government/Other IDs: SocialSecurity: 307-61-2568; IDNum (Passport): MetroPlus Health W31789908; License (Driver): SU N79623808↵Telehealth metadata: IPAddress: 203.0.113.108 — Url (portal): https://portal18.example.org/ejwffz/T-4356-48002↵Devices: Home nebulizer Device OB-ZC-97B8-988; Apple Watch Device S/P DH-7ZI9-0058↵Biometric: BioID retinal scan ID ZV-2334 captured for secure sign-in↵Vehicle: 2018 Horizon Academy — VIN: 9GQRT13349I760908 — Plate: SU 3ZM-J308↵LocationOther: McKenzie Pass (patient reports brisk walking here daily)↵Unknown: Internal Note Token: YN-5858-UNCL↵Assessment/Plan: Persistent cough improving; CXR consistent with bronchitis. Continue albuterol via nebulizer, azithromycin day 4/5.',
'Memo 27 — Cardiology (Hypertension Check)↵Date: 2025-10-03↵Hospital: Riverside Medical Pavilion (Outpatient Clinic)↵Doctor: Dr. Wesson Arkwright, MD — License: QY-APC-829189; Email: drwessonarkwright@hospital.example; Fax: (108) 5435-9606↵Patient: Jamal “Jamal” Hughes — Age: 48 — Profession: Teacher Organization: Tailspin Toys↵Address: 950 Maple Ave, City: Ann Arbor, State: MI, Zip: 61888, CountryOrRegion: United States↵Phone: (108) 99617-8617 — Email: jamal.hughes@example.com — Username: jhughes57↵MedicalRecord: MRN W-1699-47317 — HealthPlan: Bright Health HMO ID NN-4345-4487 — Account: NM-7669627117↵Government/Other IDs: SocialSecurity: 307-61-2577; IDNum (Passport): MetroPlus Health W31789917; License (Driver): SU N79623817↵Telehealth metadata: IPAddress: 203.0.113.117 — Url (portal): https://portal27.example.org/ejwffz/T-4356-48011↵Devices: Home nebulizer Device OB-ZC-97B8-997; Apple Watch Device S/P DH-7ZI9-0067↵Biometric: BioID retinal scan ID ZV-2343 captured for secure sign-in↵Vehicle: 2012 Horizon Academy — VIN: 9GQRT13349I760917 — Plate: SU 3ZM-J317↵LocationOther: Greenbelt Park (light daily jogs)↵Unknown: Internal Note Token: YN-58517-UNCL↵Assessment/Plan: BP trending down; continue lisinopril; low-sodium diet reinforced; follow-up BMP in 2 weeks.',
'Memo 9 — Neurology (Migraine Prophylaxis)↵Date: 2025-07-30↵Hospital: Richmond Hospital Neurology Group↵Doctor: Dr. Trista Beltran, MD — License: RM-JCYCD-596450 — Email: w.mxvjhp@lqwecdzhxjk.example — Fax: (108) 8024-8713↵Patient: Tamera Sackville — Age: 40 — Profession: Funeral Furnisher Organization: Brockville General Hospital↵Address: 36 Bigler Blvd, City: Breedsville, State: SD, Zip: 67661, CountryOrRegion: United States↵Phone: (108) 8623-5804 — Email: stupendousfriend129@live.com — Username: q↵MedicalRecord: MRN U-236195 — HealthPlan: NYU Grossman Hospital ID YY-9067-7258 — Account: RHX-TXZX-3727↵IDs: SocialSecurity: 315-39-1817; IDNum (Passport): MetroPlus Health J74634784; License (Driver): Oscar Health QV-85583758↵Connectivity: IPAddress: 192.0.2.190 — Url: https://iebsj.irjymcroxasfz.org/U-236195↵Devices: Smart headache diary Device HD-1 SN HD1-5933; Nightguard Device NG-75 SN NG-0137↵BioID: BioID voiceprint HO-9694 used for telephone refills↵Vehicle: Tesla Model 3 — VIN: 4IX3Y5LM2XP843537 — Plate: MA NEURA-1↵LocationOther: Lloyd Lake (photosensitivity triggers noted)↵Unknown: MiscRef NEU-X1↵Assessment/Plan: Start propranolol 40 mg BID; riboflavin and magnesium supplements; follow-up in 6 weeks.',
'Memo 17 — Orthopedics (Knee Pain Evaluation)↵Date: 2025-09-23↵Hospital: Harborview Medical Center (Outpatient Clinic)↵Doctor: Dr. Trista Beltran, MD — License: QY-APC-829189; Email: drtristabeltran@hospital.example; Fax: (108) 5435-9606↵Patient: Jamal “Jamal” Hughes — Age: 50 — Profession: Manager Organization: Northwind Health↵Address: 940 Willow Rd, City: Ann Arbor, State: MI, Zip: 61878, CountryOrRegion: United States↵Phone: (108) 99607-8607 — Email: jamal.hughes@example.com — Username: jhughes47↵MedicalRecord: MRN W-1699-47307 — HealthPlan: Bright Health HMO ID NN-4345-4477 — Account: NM-7669627107↵Government/Other IDs: SocialSecurity: 307-61-2567; IDNum (Passport): MetroPlus Health W31789907; License (Driver): SU N79623807↵Telehealth metadata: IPAddress: 203.0.113.107 — Url (portal): https://portal17.example.org/ejwffz/T-4356-48001↵Devices: Home nebulizer Device OB-ZC-97B8-987; Apple Watch Device S/P DH-7ZI9-0057↵Biometric: BioID retinal scan ID ZV-2333 captured for secure sign-in↵Vehicle: 2017 Horizon Academy — VIN: 9GQRT13349I760907 — Plate: SU 3ZM-J307↵LocationOther: Downtown Loop (commuter walking)↵Unknown: Internal Note Token: YN-5857-UNCL↵Assessment/Plan: Knee pain likely overuse; RICE, PT referral; NSAIDs PRN; imaging if persists.',
'Memo 26 — General Medicine (Bronchitis Follow-up)↵Date: 2025-10-02↵Hospital: Cedar Grove Hospital (Outpatient Clinic)↵Doctor: Dr. Trista Beltran, MD — License: QY-APC-829189; Email: drtristabeltran@hospital.example; Fax: (108) 5435-9606↵Patient: Iris “Iris” Ahmed — Age: 47 — Profession: Engineer Organization: Proseware Labs↵Address: 949 Holladay Ln, City: Boulder, State: CO, Zip: 61887, CountryOrRegion: United States↵Phone: (108) 99616-8616 — Email: iris.ahmed@example.com — Username: iahmed56↵MedicalRecord: MRN W-1699-47316 — HealthPlan: Bright Health HMO ID NN-4345-4486 — Account: NM-7669627116↵Government/Other IDs: SocialSecurity: 307-61-2576; IDNum (Passport): MetroPlus Health W31789916; License (Driver): SU N79623816↵Telehealth metadata: IPAddress: 203.0.113.116 — Url (portal): https://portal26.example.org/ejwffz/T-4356-48010↵Devices: Home nebulizer Device OB-ZC-97B8-996; Apple Watch Device S/P DH-7ZI9-0066↵Biometric: BioID retinal scan ID ZV-2342 captured for secure sign-in↵Vehicle: 2011 Horizon Academy — VIN: 9GQRT13349I760916 — Plate: SU 3ZM-J316↵LocationOther: McKenzie Pass (patient reports brisk walking here daily)↵Unknown: Internal Note Token: YN-58516-UNCL↵Assessment/Plan: Persistent cough improving; CXR consistent with bronchitis. Continue albuterol via nebulizer, azithromycin day 2/5.',
'Memo 3 — Endocrinology (Type 1 Diabetes Titration)↵Date: 2025-09-04↵Hospital: Johnston-Willis Hospital Endocrinology↵Doctor: Dr. Perla Swartz, MD — License: RE-EDQ-977751 — Email: p.nandakumar@endocrine.example — Fax: (108) 7866-2893↵Patient: Matheo Matsuoka — Age: 35 — Profession: Jewelery Maker Organization: Hulu↵Address: 6 Nido Pl, City: Wonder Lake, State: SD, Zip: 91905, CountryOrRegion: United States↵Phone: (108) 3638-5128 — Email: stupendousmusician94@live.com — Username: e_dev↵MedicalRecord: MRN Z-51639 — HealthPlan: Golden Rule Choice POS II ID QO-6128-4270 — Account: MLF-0444-0604↵IDs: SocialSecurity: 679-84-2963; IDNum (Passport): MetroPlus Health V91906709; License (Driver): SU U79862448↵Tech: Televisit IPAddress: 192.0.2.56 — Url: https://ksjr.irjymcroxasfz.org/ycfhu/T-2899↵Devices: Device Dexcom G6 SN D6-441-264; Insulin pump Device Tandem t:slim S/N TS-922-404↵BioID: BioID voiceprint PU-9334 enrolled for pump support line↵Vehicle: Toyota Prius — VIN: QTWBP37A386543558 — Plate: MA ECO-992↵LocationOther: Beaverhead Mountains Esplanade (pre-meal run pattern)↵Unknown: CaseToken TM-86L↵Assessment/Plan: A1c 8.2%. Adjusted basal rates; reviewed carb ratios. Hypoglycemia protocol reinforced.',
'Memo 15 — Neurology (Migraine Follow-up)↵Date: 2025-09-21↵Hospital: Riverside Medical Pavilion (Outpatient Clinic)↵Doctor: Dr. Perla Swartz, MD — License: QY-APC-829189; Email: drperlaswartz@hospital.example; Fax: (108) 5435-9606↵Patient: Marcus “Marcus” Klein — Age: 48 — Profession: Technician Organization: Tailspin Toys↵Address: 938 Birch Way, City: Salem, State: OR, Zip: 61876, CountryOrRegion: United States↵Phone: (108) 99605-8605 — Email: marcus.klein@example.com — Username: mklein45↵MedicalRecord: MRN W-1699-47305 — HealthPlan: Bright Health HMO ID NN-4345-4475 — Account: NM-7669627105↵Government/Other IDs: SocialSecurity: 307-61-2565; IDNum (Passport): MetroPlus Health W31789905; License (Driver): SU N79623805↵Telehealth metadata: IPAddress: 203.0.113.105 — Url (portal): https://portal15.example.org/ejwffz/T-4356-47999↵Devices: Home nebulizer Device OB-ZC-97B8-985; Apple Watch Device S/P DH-7ZI9-0055↵Biometric: BioID retinal scan ID ZV-2331 captured for secure sign-in↵Vehicle: 2015 Horizon Academy — VIN: 9GQRT13349I760905 — Plate: SU 3ZM-J305↵LocationOther: Greenbelt Park (light daily jogs)↵Unknown: Internal Note Token: YN-5855-UNCL↵Assessment/Plan: Migraine frequency reduced; continue prophylaxis; maintain trigger diary.',
'Memo 24 — Gastroenterology (GERD Management)↵Date: 2025-09-30↵Hospital: Mercy General Hospital (Outpatient Clinic)↵Doctor: Dr. Perla Swartz, MD — License: QY-APC-829189; Email: drperlaswartz@hospital.example; Fax: (108) 5435-9606↵Patient: Elena “Elena” Santos — Age: 45 — Profession: Developer Organization: Contoso Builders↵Address: 947 Oak Blvd, City: Madison, State: WI, Zip: 61885, CountryOrRegion: United States↵Phone: (108) 99614-8614 — Email: elena.santos@example.com — Username: esantos54↵MedicalRecord: MRN W-1699-47314 — HealthPlan: Bright Health HMO ID NN-4345-4484 — Account: NM-7669627114↵Government/Other IDs: SocialSecurity: 307-61-2574; IDNum (Passport): MetroPlus Health W31789914; License (Driver): SU N79623814↵Telehealth metadata: IPAddress: 203.0.113.114 — Url (portal): https://portal24.example.org/ejwffz/T-4356-48008↵Devices: Home nebulizer Device OB-ZC-97B8-994; Apple Watch Device S/P DH-7ZI9-0064↵Biometric: BioID retinal scan ID ZV-2340 captured for secure sign-in↵Vehicle: 2024 Horizon Academy — VIN: 9GQRT13349I760914 — Plate: SU 3ZM-J314↵LocationOther: Riverside Trail (evening strolls)↵Unknown: Internal Note Token: YN-58514-UNCL↵Assessment/Plan: Symptoms of GERD improved; continue PPI; lifestyle counseling provided.',
'Memo 33 — Orthopedics (Knee Pain Evaluation)↵Date: 2025-10-09↵Hospital: Riverside Medical Pavilion (Outpatient Clinic)↵Doctor: Dr. Perla Swartz, MD — License: QY-APC-829189; Email: drperlaswartz@hospital.example; Fax: (108) 5435-9606↵Patient: Nora “Nora” Patel — Age: 54 — Profession: Manager Organization: Tailspin Toys↵Address: 956 Willow Rd, City: Raleigh, State: NC, Zip: 61894, CountryOrRegion: United States↵Phone: (108) 99623-8623 — Email: nora.patel@example.com — Username: npatel63↵MedicalRecord: MRN W-1699-47323 — HealthPlan: Bright Health HMO ID NN-4345-4493 — Account: NM-7669627123↵Government/Other IDs: SocialSecurity: 307-61-2583; IDNum (Passport): MetroPlus Health W31789923; License (Driver): SU N79623823↵Telehealth metadata: IPAddress: 203.0.113.123 — Url (portal): https://portal33.example.org/ejwffz/T-4356-48017↵Devices: Home nebulizer Device OB-ZC-97B8-9103; Apple Watch Device S/P DH-7ZI9-0073↵Biometric: BioID retinal scan ID ZV-2349 captured for secure sign-in↵Vehicle: 2018 Horizon Academy — VIN: 9GQRT13349I760923 — Plate: SU 3ZM-J323↵LocationOther: Downtown Loop (commuter walking)↵Unknown: Internal Note Token: YN-58523-UNCL↵Assessment/Plan: Knee pain likely overuse; RICE, PT referral; NSAIDs PRN; imaging if persists.',
'Memo 4 — Orthopedics (ACL Tear Rehab)↵Date: 2025-07-19↵Hospital: Peninsula Regional Medical Center↵Doctor: Dr. Monroe Koster, MD — License: BR-VQFQJ-891705 — Email: central peace health center.wci@wrreroqsoqkg.example — Fax: (108) 9105-2230↵Patient: Mustafa Betts — Age: 54 — Profession: Psychologist Organization: Sanderson Farms Union Memorial Hospital↵Address: 56 Ravenwood St, City: Wonder Lake, State: SD, Zip: 96705, CountryOrRegion: United States↵Phone: (108) 9968-4444 — Email: stupendousbrother95@live.com — Username: d_rwewjr65↵MedicalRecord: MNO SV-47144 — HealthPlan: Harborview Medical Center HealthPlus ID FBD-9593-21 — Account: KWP-56353436↵IDs: SocialSecurity: 619-66-3071; IDNum (Passport): MetroPlus Health B61784124; License (Driver): MA WVC-583008↵Network: IPAddress: 203.0.113.199 — Url: https://ejubp.irjymcroxasfz.org/SV-47144↵Devices: Hinged knee brace Device SK-PA-04590; Home TENS Device VPP-91 FP D61-8618↵BioID: BioID retinal scan FR-2819 at imaging desk↵Vehicle: Ford K-286 — VIN: 3UNKF8GU9GZJ42583 — Plate: SU NVJ-265↵LocationOther: Mount Jefferson (injury occurred ascending stairs)↵Unknown: RefCode M-CB-709↵Assessment/Plan: Partial ACL tear; PT 2x/week x 6 weeks, brace use reviewed; consider surgical consult if instability persists.',
'Memo 14 — Dermatology (Rash Evaluation)↵Date: 2025-09-20↵Hospital: Cedar Grove Hospital (Outpatient Clinic)↵Doctor: Dr. Monroe Koster, MD — License: QY-APC-829189; Email: drmonroekoster@hospital.example; Fax: (108) 5435-9606↵Patient: Elena “Elena” Santos — Age: 47 — Profession: Designer Organization: Proseware Labs↵Address: 937 Cedar Ct, City: Madison, State: WI, Zip: 61875, CountryOrRegion: United States↵Phone: (108) 99604-8604 — Email: elena.santos@example.com — Username: esantos44↵MedicalRecord: MRN W-1699-47304 — HealthPlan: Bright Health HMO ID NN-4345-4474 — Account: NM-7669627104↵Government/Other IDs: SocialSecurity: 307-61-2564; IDNum (Passport): MetroPlus Health W31789904; License (Driver): SU N79623804↵Telehealth metadata: IPAddress: 203.0.113.104 — Url (portal): https://portal14.example.org/ejwffz/T-4356-47998↵Devices: Home nebulizer Device OB-ZC-97B8-984; Apple Watch Device S/P DH-7ZI9-0054↵Biometric: BioID retinal scan ID ZV-2330 captured for secure sign-in↵Vehicle: 2014 Horizon Academy — VIN: 9GQRT13349I760904 — Plate: SU 3ZM-J304↵LocationOther: McKenzie Pass (patient reports brisk walking here daily)↵Unknown: Internal Note Token: YN-5854-UNCL↵Assessment/Plan: Topical steroid started for rash; avoid irritants; recheck in 10 days.',
'Memo 23 — Neurology (Migraine Follow-up)↵Date: 2025-09-29↵Hospital: Harborview Medical Center (Outpatient Clinic)↵Doctor: Dr. Monroe Koster, MD — License: QY-APC-829189; Email: drmonroekoster@hospital.example; Fax: (108) 5435-9606↵Patient: Nora “Nora” Patel — Age: 44 — Profession: Technician Organization: Northwind Health↵Address: 946 Birch Way, City: Raleigh, State: NC, Zip: 61884, CountryOrRegion: United States↵Phone: (108) 99613-8613 — Email: nora.patel@example.com — Username: npatel53↵MedicalRecord: MRN W-1699-47313 — HealthPlan: Bright Health HMO ID NN-4345-4483 — Account: NM-7669627113↵Government/Other IDs: SocialSecurity: 307-61-2573; IDNum (Passport): MetroPlus Health W31789913; License (Driver): SU N79623813↵Telehealth metadata: IPAddress: 203.0.113.113 — Url (portal): https://portal23.example.org/ejwffz/T-4356-48007↵Devices: Home nebulizer Device OB-ZC-97B8-993; Apple Watch Device S/P DH-7ZI9-0063↵Biometric: BioID retinal scan ID ZV-2339 captured for secure sign-in↵Vehicle: 2023 Horizon Academy — VIN: 9GQRT13349I760913 — Plate: SU 3ZM-J313↵LocationOther: Greenbelt Park (light daily jogs)↵Unknown: Internal Note Token: YN-58513-UNCL↵Assessment/Plan: Migraine frequency reduced; continue prophylaxis; maintain trigger diary.',
'Memo 32 — Gastroenterology (GERD Management)↵Date: 2025-10-08↵Hospital: Cedar Grove Hospital (Outpatient Clinic)↵Doctor: Dr. Monroe Koster, MD — License: QY-APC-829189; Email: drmonroekoster@hospital.example; Fax: (108) 5435-9606↵Patient: Samuel “Samuel” Ortiz — Age: 53 — Profession: Developer Organization: Proseware Labs↵Address: 955 Oak Blvd, City: Dover, State: DE, Zip: 61893, CountryOrRegion: United States↵Phone: (108) 99622-8622 — Email: samuel.ortiz@example.com — Username: sortiz62↵MedicalRecord: MRN W-1699-47322 — HealthPlan: Bright Health HMO ID NN-4345-4492 — Account: NM-7669627122↵Government/Other IDs: SocialSecurity: 307-61-2582; IDNum (Passport): MetroPlus Health W31789922; License (Driver): SU N79623822↵Telehealth metadata: IPAddress: 203.0.113.122 — Url (portal): https://portal32.example.org/ejwffz/T-4356-48016↵Devices: Home nebulizer Device OB-ZC-97B8-9102; Apple Watch Device S/P DH-7ZI9-0072↵Biometric: BioID retinal scan ID ZV-2348 captured for secure sign-in↵Vehicle: 2017 Horizon Academy — VIN: 9GQRT13349I760922 — Plate: SU 3ZM-J322↵LocationOther: Riverside Trail (evening strolls)↵Unknown: Internal Note Token: YN-58522-UNCL↵Assessment/Plan: Symptoms of GERD improved; continue PPI; lifestyle counseling provided.',
'Memo 10 — General Surgery (Appendectomy Post‑Op)↵Date: 2025-08-28↵Hospital: Howard Hospital↵Doctor: Dr. Kwame M’Meeuwis, MD — License: YE-FXZX-019662 — Email: b.vyjjsp@ybtzmbnnhsg.example — Fax: (108) 6463-1094↵Patient: Angella Tudor — Age: 24 — Profession: Physicist Organization: Inova Alexandria Hospital↵Address: 62 Galewood Dr, City: Lost Lake Woods, State: SD, Zip: 19496, CountryOrRegion: United States↵Phone: (108) 5945-4598 — Email: stupendousdragonfly922@live.com — Username: gqtb_w59↵MedicalRecord: MRN VD-52924 — HealthPlan: Memorial Hospital HMO ID BQ-1023-6301 — Account: UN-PGC-22122↵IDs: SocialSecurity: 743-32-5550; IDNum (Passport): MetroPlus Health Z43703051; License (Driver): Oscar Health QB-69231898↵Net/Portal: IPAddress: 203.0.113.12 — Url: https://yycvycs.irjymcroxasfz.org/VD-52924↵Devices: Wound vac Device WV-10 SN BG-40-976; Home thermometer Device SQ-0 SN KL4-0366↵BioID: BioID fingerprint EF-6109 for medication locker access↵Vehicle: Honda Fit — VIN: KZRCH38657M415568 — Plate: SU MHFU-77↵LocationOther: Symmetry Spire (walking tolerated 20 minutes)↵Unknown: SURG-SES-7G↵Assessment/Plan: Incisions clean/dry/intact; afebrile; continue oral antibiotics; avoid heavy lifting for 2 weeks; return precautions reviewed.',
'Memo 13 — Pulmonology (Asthma Follow-up)↵Date: 2025-09-19↵Hospital: St. Raphael Clinic (Outpatient Clinic)↵Doctor: Dr. Kwame M’Meeuwis, MD — License: QY-APC-829189; Email: drkwamemmeeuwis@hospital.example; Fax: (108) 5435-9606↵Patient: Nora “Nora” Patel — Age: 46 — Profession: Analyst Organization: Fabrikam Foods↵Address: 936 Elm Dr, City: Raleigh, State: NC, Zip: 61874, CountryOrRegion: United States↵Phone: (108) 99603-8603 — Email: nora.patel@example.com — Username: npatel43↵MedicalRecord: MRN W-1699-47303 — HealthPlan: Bright Health HMO ID NN-4345-4473 — Account: NM-7669627103↵Government/Other IDs: SocialSecurity: 307-61-2563; IDNum (Passport): MetroPlus Health W31789903; License (Driver): SU N79623803↵Telehealth metadata: IPAddress: 203.0.113.103 — Url (portal): https://portal13.example.org/ejwffz/T-4356-47997↵Devices: Home nebulizer Device OB-ZC-97B8-983; Apple Watch Device S/P DH-7ZI9-0053↵Biometric: BioID retinal scan ID ZV-2329 captured for secure sign-in↵Vehicle: 2013 Horizon Academy — VIN: 9GQRT13349I760903 — Plate: SU 3ZM-J303↵LocationOther: Downtown Loop (commuter walking)↵Unknown: Internal Note Token: YN-5853-UNCL↵Assessment/Plan: Asthma stable on current regimen; refill inhaler; spacer technique reviewed.',
'Memo 22 — Dermatology (Rash Evaluation)↵Date: 2025-09-28↵Hospital: Keck Hospital (Outpatient Clinic)↵Doctor: Dr. Kwame M’Meeuwis, MD — License: QY-APC-829189; Email: drkwamemmeeuwis@hospital.example; Fax: (108) 5435-9606↵Patient: Samuel “Samuel” Ortiz — Age: 43 — Profession: Designer Organization: Seaside Market↵Address: 945 Cedar Ct, City: Dover, State: DE, Zip: 61883, CountryOrRegion: United States↵Phone: (108) 99612-8612 — Email: samuel.ortiz@example.com — Username: sortiz52↵MedicalRecord: MRN W-1699-47312 — HealthPlan: Bright Health HMO ID NN-4345-4482 — Account: NM-7669627112↵Government/Other IDs: SocialSecurity: 307-61-2572; IDNum (Passport): MetroPlus Health W31789912; License (Driver): SU N79623812↵Telehealth metadata: IPAddress: 203.0.113.112 — Url (portal): https://portal22.example.org/ejwffz/T-4356-48006↵Devices: Home nebulizer Device OB-ZC-97B8-992; Apple Watch Device S/P DH-7ZI9-0062↵Biometric: BioID retinal scan ID ZV-2338 captured for secure sign-in↵Vehicle: 2022 Horizon Academy — VIN: 9GQRT13349I760912 — Plate: SU 3ZM-J312↵LocationOther: McKenzie Pass (patient reports brisk walking here daily)↵Unknown: Internal Note Token: YN-58512-UNCL↵Assessment/Plan: Topical steroid started for rash; avoid irritants; recheck in 10 days.',
'Memo 31 — Neurology (Migraine Follow-up)↵Date: 2025-10-07↵Hospital: St. Raphael Clinic (Outpatient Clinic)↵Doctor: Dr. Kwame M’Meeuwis, MD — License: QY-APC-829189; Email: drkwamemmeeuwis@hospital.example; Fax: (108) 5435-9606↵Patient: Avery “Avery” Chen — Age: 52 — Profession: Technician Organization: Fabrikam Foods↵Address: 954 Birch Way, City: Burlingame, State: CA, Zip: 61892, CountryOrRegion: United States↵Phone: (108) 99621-8621 — Email: avery.chen@example.com — Username: achen61↵MedicalRecord: MRN W-1699-47321 — HealthPlan: Bright Health HMO ID NN-4345-4491 — Account: NM-7669627121↵Government/Other IDs: SocialSecurity: 307-61-2581; IDNum (Passport): MetroPlus Health W31789921; License (Driver): SU N79623821↵Telehealth metadata: IPAddress: 203.0.113.121 — Url (portal): https://portal31.example.org/ejwffz/T-4356-48015↵Devices: Home nebulizer Device OB-ZC-97B8-9101; Apple Watch Device S/P DH-7ZI9-0071↵Biometric: BioID retinal scan ID ZV-2347 captured for secure sign-in↵Vehicle: 2016 Horizon Academy — VIN: 9GQRT13349I760921 — Plate: SU 3ZM-J321↵LocationOther: Greenbelt Park (light daily jogs)↵Unknown: Internal Note Token: YN-58521-UNCL↵Assessment/Plan: Migraine frequency reduced; continue prophylaxis; maintain trigger diary.',
'Memo 7 — Gastroenterology (IBS Flare)↵Date: 2025-08-06↵Hospital: FIU Hospital↵Doctor: Dr. Antionette Klerkx, MD — License: JV-AN-739161 — Email: h.mlhvfx@uuaivecwywf.example — Fax: (108) 4737-2790↵Patient: Darron Moreno — Age: 32 — Profession: Sports Coach Organization: Peak Performance Gym Good Samaritan Hospital↵Address: 935 Winfield Way, City: Lake Wales, State: SD, Zip: 12563, CountryOrRegion: United States↵Phone: (108) 5476-3278 — Email: stupendouseditor764@live.com — Username: stupendousbrother234.auudqqpn82↵MedicalRecord: MRN GM-94955 — HealthPlan: CalOptima ID OJ-4044-3289 — Account: LR-DQCT-4375↵IDs: SocialSecurity: 282-78-2909; IDNum (Passport): MetroPlus Health T56439506; License (Driver): SU H38267299↵Network: IPAddress: 203.0.113.78 — Url: https://bp.irjymcroxasfz.org/GM-94955↵Devices: Home BP monitor Device BP-SN-90067; Smart scale Device SS-200 SN SS-004-486↵BioID: BioID palm vein template PV-9920 (building access)↵Vehicle: Sunset Park — VIN: 9X3IY7UO9GU058074 — Plate: Chandler Regional Medical Center BF-7809↵LocationOther: Mount Sinai Beth Israel (symptoms worsened after event)↵Unknown: GI-KSC-E8↵Assessment/Plan: IBS-D flare; low-FODMAP diet guidance; trial rifaximin; stool studies ordered.',
'Memo 10 — General Medicine (Bronchitis Follow-up)↵Date: 2025-09-16↵Hospital: Keck Hospital (Outpatient Clinic)↵Doctor: Dr. Antionette Klerkx, MD — License: QY-APC-829189; Email: drantionetteklerkx@hospital.example; Fax: (108) 5435-9606↵Patient: Willard “Willard” Nardi — Age: 43 — Profession: Engineer Organization: Seaside Market↵Address: 933 Holladay Ln, City: Wonder Lake, State: SD, Zip: 61871, CountryOrRegion: United States↵Phone: (108) 99600-8600 — Email: willard.nardi@example.com — Username: wnardi40↵MedicalRecord: MRN W-1699-47300 — HealthPlan: Bright Health HMO ID NN-4345-4470 — Account: NM-7669627100↵Government/Other IDs: SocialSecurity: 307-61-2560; IDNum (Passport): MetroPlus Health W31789900; License (Driver): SU N79623800↵Telehealth metadata: IPAddress: 203.0.113.100 — Url (portal): https://portal10.example.org/ejwffz/T-4356-47994↵Devices: Home nebulizer Device OB-ZC-97B8-980; Apple Watch Device S/P DH-7ZI9-0050↵Biometric: BioID retinal scan ID ZV-2326 captured for secure sign-in↵Vehicle: 2010 Horizon Academy — VIN: 9GQRT13349I760900 — Plate: SU 3ZM-J300↵LocationOther: McKenzie Pass (patient reports brisk walking here daily)↵Unknown: Internal Note Token: YN-5850-UNCL↵Assessment/Plan: Persistent cough improving; CXR consistent with bronchitis. Continue albuterol via nebulizer, azithromycin day 1/5.',
'Memo 19 — Cardiology (Hypertension Check)↵Date: 2025-09-25↵Hospital: St. Raphael Clinic (Outpatient Clinic)↵Doctor: Dr. Antionette Klerkx, MD — License: QY-APC-829189; Email: drantionetteklerkx@hospital.example; Fax: (108) 5435-9606↵Patient: Diego “Diego” Kim — Age: 52 — Profession: Teacher Organization: Fabrikam Foods↵Address: 942 Maple Ave, City: Gainesville, State: FL, Zip: 61880, CountryOrRegion: United States↵Phone: (108) 99609-8609 — Email: diego.kim@example.com — Username: dkim49↵MedicalRecord: MRN W-1699-47309 — HealthPlan: Bright Health HMO ID NN-4345-4479 — Account: NM-7669627109↵Government/Other IDs: SocialSecurity: 307-61-2569; IDNum (Passport): MetroPlus Health W31789909; License (Driver): SU N79623809↵Telehealth metadata: IPAddress: 203.0.113.109 — Url (portal): https://portal19.example.org/ejwffz/T-4356-48003↵Devices: Home nebulizer Device OB-ZC-97B8-989; Apple Watch Device S/P DH-7ZI9-0059↵Biometric: BioID retinal scan ID ZV-2335 captured for secure sign-in↵Vehicle: 2019 Horizon Academy — VIN: 9GQRT13349I760909 — Plate: SU 3ZM-J309↵LocationOther: Greenbelt Park (light daily jogs)↵Unknown: Internal Note Token: YN-5859-UNCL↵Assessment/Plan: BP trending down; continue lisinopril; low-sodium diet reinforced; follow-up BMP in 2 weeks.',
'Memo 28 — Endocrinology (Diabetes Management)↵Date: 2025-10-04↵Hospital: Keck Hospital (Outpatient Clinic)↵Doctor: Dr. Antionette Klerkx, MD — License: QY-APC-829189; Email: drantionetteklerkx@hospital.example; Fax: (108) 5435-9606↵Patient: Priya “Priya” Garcia — Age: 49 — Profession: Nurse Organization: Seaside Market↵Address: 951 Pine St, City: Reno, State: NV, Zip: 61889, CountryOrRegion: United States↵Phone: (108) 99618-8618 — Email: priya.garcia@example.com — Username: pgarcia58↵MedicalRecord: MRN W-1699-47318 — HealthPlan: Bright Health HMO ID NN-4345-4488 — Account: NM-7669627118↵Government/Other IDs: SocialSecurity: 307-61-2578; IDNum (Passport): MetroPlus Health W31789918; License (Driver): SU N79623818↵Telehealth metadata: IPAddress: 203.0.113.118 — Url (portal): https://portal28.example.org/ejwffz/T-4356-48012↵Devices: Home nebulizer Device OB-ZC-97B8-998; Apple Watch Device S/P DH-7ZI9-0068↵Biometric: BioID retinal scan ID ZV-2344 captured for secure sign-in↵Vehicle: 2013 Horizon Academy — VIN: 9GQRT13349I760918 — Plate: SU 3ZM-J318↵LocationOther: Riverside Trail (evening strolls)↵Unknown: Internal Note Token: YN-58518-UNCL↵Assessment/Plan: A1c stable; continue metformin; reinforce diet & exercise; CGM review next visit.',
'Memo 1 — General Medicine (Bronchitis Follow-up)↵Date: 2025-09-16↵Hospital: Keck Hospital (Outpatient Clinic)↵Doctor: Dr. Harley Muir, MD — License: QY-APC-829189; Email: l.vmzg@kxyvoipexathn.example; Fax: (108) 5435-9606↵Patient: Willard “Willard” Nardi — Age: 43 — Profession: Engineer Organization: Seaside Market↵Address: 933 Holladay Ln, City: Wonder Lake, State: SD, Zip: 61871, CountryOrRegion: United States↵Phone: (108) 9670-8629 — Email: stupendousparent414@live.com — Username: vdmgf60↵MedicalRecord: MRN W-1699-47309 — HealthPlan: Bright Health HMO ID NN-4345-4471 — Account: NM-7669627018↵Government/Other IDs: SocialSecurity: 307-61-2566; IDNum (Passport): MetroPlus Health W31789908; License (Driver): SU N79623857↵Telehealth metadata: IPAddress: 203.0.113.45 — Url (portal): https://jbumkk.irjymcroxasfz.org/ejwffz/T-4356-47994↵Devices: Home nebulizer Device OB-ZC-97B8-988; Apple Watch Device S/P DH-7ZI9-0058↵Biometric: BioID retinal scan ID ZV-2326 captured for secure sign-in↵Vehicle: 2013 Horizon Academy — VIN: 9GQRT13349I760956 — Plate: SU 3ZM-J73↵LocationOther: McKenzie Pass (patient reports brisk walking here daily)↵Unknown: Internal Note Token: YN-5850-UNCL↵Assessment/Plan: Persistent cough improving; CXR consistent with bronchitis. Continue albuterol via nebulizer SN-NB-71F8-112, azithromycin day 3/5. School exposure likely. Return if fever > 101.5°F or dyspnea.',
'Memo 11 — Cardiology (Hypertension Check)↵Date: 2025-09-17↵Hospital: Harborview Medical Center (Outpatient Clinic)↵Doctor: Dr. Harley Muir, MD — License: QY-APC-829189; Email: drharleymuir@hospital.example; Fax: (108) 5435-9606↵Patient: Avery “Avery” Chen — Age: 44 — Profession: Teacher Organization: Northwind Health↵Address: 934 Maple Ave, City: Burlingame, State: CA, Zip: 61872, CountryOrRegion: United States↵Phone: (108) 99601-8601 — Email: avery.chen@example.com — Username: achen41↵MedicalRecord: MRN W-1699-47301 — HealthPlan: Bright Health HMO ID NN-4345-4471 — Account: NM-7669627101↵Government/Other IDs: SocialSecurity: 307-61-2561; IDNum (Passport): MetroPlus Health W31789901; License (Driver): SU N79623801↵Telehealth metadata: IPAddress: 203.0.113.101 — Url (portal): https://portal11.example.org/ejwffz/T-4356-47995↵Devices: Home nebulizer Device OB-ZC-97B8-981; Apple Watch Device S/P DH-7ZI9-0051↵Biometric: BioID retinal scan ID ZV-2327 captured for secure sign-in↵Vehicle: 2011 Horizon Academy — VIN: 9GQRT13349I760901 — Plate: SU 3ZM-J301↵LocationOther: Greenbelt Park (light daily jogs)↵Unknown: Internal Note Token: YN-5851-UNCL↵Assessment/Plan: BP trending down; continue lisinopril; low-sodium diet reinforced; follow-up BMP in 2 weeks.',
'Memo 20 — Endocrinology (Diabetes Management)↵Date: 2025-09-26↵Hospital: Cedar Grove Hospital (Outpatient Clinic)↵Doctor: Dr. Harley Muir, MD — License: QY-APC-829189; Email: drharleymuir@hospital.example; Fax: (108) 5435-9606↵Patient: Willard “Willard” Nardi — Age: 53 — Profession: Nurse Organization: Proseware Labs↵Address: 943 Pine St, City: Wonder Lake, State: SD, Zip: 61881, CountryOrRegion: United States↵Phone: (108) 99610-8610 — Email: willard.nardi@example.com — Username: wnardi50↵MedicalRecord: MRN W-1699-47310 — HealthPlan: Bright Health HMO ID NN-4345-4480 — Account: NM-7669627110↵Government/Other IDs: SocialSecurity: 307-61-2570; IDNum (Passport): MetroPlus Health W31789910; License (Driver): SU N79623810↵Telehealth metadata: IPAddress: 203.0.113.110 — Url (portal): https://portal20.example.org/ejwffz/T-4356-48004↵Devices: Home nebulizer Device OB-ZC-97B8-990; Apple Watch Device S/P DH-7ZI9-0060↵Biometric: BioID retinal scan ID ZV-2336 captured for secure sign-in↵Vehicle: 2020 Horizon Academy — VIN: 9GQRT13349I760910 — Plate: SU 3ZM-J310↵LocationOther: Riverside Trail (evening strolls)↵Unknown: Internal Note Token: YN-58510-UNCL↵Assessment/Plan: A1c stable; continue metformin; reinforce diet & exercise; CGM review next visit.',
'Memo 29 — Pulmonology (Asthma Follow-up)↵Date: 2025-10-05↵Hospital: Harborview Medical Center (Outpatient Clinic)↵Doctor: Dr. Harley Muir, MD — License: QY-APC-829189; Email: drharleymuir@hospital.example; Fax: (108) 5435-9606↵Patient: Diego “Diego” Kim — Age: 50 — Profession: Analyst Organization: Northwind Health↵Address: 952 Elm Dr, City: Gainesville, State: FL, Zip: 61890, CountryOrRegion: United States↵Phone: (108) 99619-8619 — Email: diego.kim@example.com — Username: dkim59↵MedicalRecord: MRN W-1699-47319 — HealthPlan: Bright Health HMO ID NN-4345-4489 — Account: NM-7669627119↵Government/Other IDs: SocialSecurity: 307-61-2579; IDNum (Passport): MetroPlus Health W31789919; License (Driver): SU N79623819↵Telehealth metadata: IPAddress: 203.0.113.119 — Url (portal): https://portal29.example.org/ejwffz/T-4356-48013↵Devices: Home nebulizer Device OB-ZC-97B8-999; Apple Watch Device S/P DH-7ZI9-0069↵Biometric: BioID retinal scan ID ZV-2345 captured for secure sign-in↵Vehicle: 2014 Horizon Academy — VIN: 9GQRT13349I760919 — Plate: SU 3ZM-J319↵LocationOther: Downtown Loop (commuter walking)↵Unknown: Internal Note Token: YN-58519-UNCL↵Assessment/Plan: Asthma stable on current regimen; refill inhaler; spacer technique reviewed.'

]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

department_list = [
    'Cardiology',
    'Cardiology',
    'Cardiology',
    'Cardiology',
    'Neurology',
    'Neurology',
    'Neurology',
    'Neurology',
    'Orthopedics',
    'Orthopedics',
    'Orthopedics',
    'Neurology',
    'Neurology',
    'Neurology',
    'Gastroenterology',
    'Gastroenterology',
    'Gastroenterology',
    'Gastroenterology',
    'Dermatology',
    'Dermatology',
    'Dermatology',
    'Dermatology',
    'Orthopedics',
    'Orthopedics',
    'Orthopedics',
    'Orthopedics',
    'Cardiology',
    'Cardiology',
    'Cardiology',
    'Cardiology',
    'Gastroenterology',
    'Gastroenterology',
    'Gastroenterology',
    'Gastroenterology'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### create table in lakehouse

doctor_table_data = []
schema = ['department','doctor','memo_content',]

for index in range(1,35):
    list_index = index - 1
    doctor_table_data.append({'doctor': doctor_list[list_index],
         'memo_content': memo_list[list_index],
        'department': department_list[list_index]
        }
    )

doctor_table_df = spark.createDataFrame(doctor_table_data, schema=schema)

display(doctor_table_df)
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write table 
"""
{'id': 'af5a29cf-f732-4bac-8fa0-395f63345887',
 'type': 'Lakehouse',
 'displayName': 'onelake_security_lh',
 'description': 'lakehouse to test onelake security V2',
 'workspaceId': 'a8cbda3d-903e-4154-97d9-9a91c95abb42'}
"""
display(doctor_table_df)

try:
    doctor_table_df.write.format('delta').mode('overwrite').save(f'abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/doctor_table')
except:
    print('ERROR')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Note the below will not work if OneLake Security has not been enabled and the identiy set to User from Delegated
# - This must be done by the person listed as the owner of the lakehouse

# CELL ********************

dir(onelake_roles)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Neuro Role
# updates: change table_path arg to table

row_constraints = onelake_roles.RowConstraint(table='doctor_table', rls_definition="SELECT * FROM doctor_table WHERE department = 'Neurology'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


row_constraints

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

column_constraints = onelake_roles.ColumnConstraint(table='doctor_table', column_names=['*'], column_effect='Permit', column_action=['Read'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

column_constraints

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

constraints = onelake_roles.Constraints(columns=column_constraints, rows=row_constraints)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

constraints.definition

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

permissions = onelake_roles.PermissionRule(path='doctor_table')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

permissions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
