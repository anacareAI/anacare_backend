#!/usr/bin/env node
import 'dotenv/config'
import { pool, log, initLog } from '../lib/db.js'

// 100+ CPTs spanning all shoppable service categories
const CATALOG = [
  // Imaging
  { cpt_code:'70450', description:'CT Head without contrast',                      category:'Imaging',       subcategory:'CT',          is_cms_shoppable:true  },
  { cpt_code:'70553', description:'MRI Brain with & without contrast',             category:'Imaging',       subcategory:'MRI',         is_cms_shoppable:true  },
  { cpt_code:'71046', description:'Chest X-Ray 2 views',                           category:'Imaging',       subcategory:'X-Ray',       is_cms_shoppable:true  },
  { cpt_code:'71250', description:'CT Chest without contrast',                     category:'Imaging',       subcategory:'CT',          is_cms_shoppable:true  },
  { cpt_code:'72141', description:'MRI Cervical Spine without contrast',           category:'Imaging',       subcategory:'MRI',         is_cms_shoppable:true  },
  { cpt_code:'72148', description:'MRI Lumbar Spine without contrast',             category:'Imaging',       subcategory:'MRI',         is_cms_shoppable:true  },
  { cpt_code:'73221', description:'MRI Upper Extremity Joint',                     category:'Imaging',       subcategory:'MRI',         is_cms_shoppable:false },
  { cpt_code:'73721', description:'MRI Lower Extremity Joint',                     category:'Imaging',       subcategory:'MRI',         is_cms_shoppable:true  },
  { cpt_code:'74177', description:'CT Abdomen & Pelvis with contrast',             category:'Imaging',       subcategory:'CT',          is_cms_shoppable:true  },
  { cpt_code:'75571', description:'CT Heart calcium scoring',                      category:'Imaging',       subcategory:'CT',          is_cms_shoppable:true  },
  { cpt_code:'76700', description:'Abdominal ultrasound, complete',                category:'Imaging',       subcategory:'Ultrasound',  is_cms_shoppable:true  },
  { cpt_code:'76830', description:'Transvaginal ultrasound',                       category:'Imaging',       subcategory:'Ultrasound',  is_cms_shoppable:true  },
  { cpt_code:'76856', description:'Pelvic ultrasound, complete',                   category:'Imaging',       subcategory:'Ultrasound',  is_cms_shoppable:true  },
  { cpt_code:'77065', description:'Diagnostic mammography, unilateral',            category:'Imaging',       subcategory:'Mammography', is_cms_shoppable:true  },
  { cpt_code:'77066', description:'Diagnostic mammography, bilateral',             category:'Imaging',       subcategory:'Mammography', is_cms_shoppable:true  },
  { cpt_code:'77067', description:'Screening mammography, bilateral',              category:'Imaging',       subcategory:'Mammography', is_cms_shoppable:true  },
  { cpt_code:'78452', description:'Myocardial perfusion imaging',                  category:'Imaging',       subcategory:'Nuclear',     is_cms_shoppable:true  },
  { cpt_code:'78816', description:'PET scan whole body',                           category:'Imaging',       subcategory:'Nuclear',     is_cms_shoppable:true  },
  { cpt_code:'71048', description:'Chest X-Ray 4+ views',                         category:'Imaging',       subcategory:'X-Ray',       is_cms_shoppable:false },
  { cpt_code:'72100', description:'X-Ray lumbar spine 2-3 views',                 category:'Imaging',       subcategory:'X-Ray',       is_cms_shoppable:false },
  { cpt_code:'73562', description:'X-Ray knee 3 views',                           category:'Imaging',       subcategory:'X-Ray',       is_cms_shoppable:false },
  // Lab
  { cpt_code:'80048', description:'Basic metabolic panel',                         category:'Lab',           subcategory:'Chemistry',   is_cms_shoppable:true  },
  { cpt_code:'80053', description:'Comprehensive metabolic panel',                 category:'Lab',           subcategory:'Chemistry',   is_cms_shoppable:true  },
  { cpt_code:'80061', description:'Lipid panel',                                   category:'Lab',           subcategory:'Chemistry',   is_cms_shoppable:true  },
  { cpt_code:'81001', description:'Urinalysis with microscopy',                    category:'Lab',           subcategory:'Urinalysis',  is_cms_shoppable:true  },
  { cpt_code:'82306', description:'Vitamin D 25-hydroxy',                         category:'Lab',           subcategory:'Chemistry',   is_cms_shoppable:true  },
  { cpt_code:'83036', description:'Hemoglobin A1c',                               category:'Lab',           subcategory:'Chemistry',   is_cms_shoppable:true  },
  { cpt_code:'84153', description:'PSA prostate specific antigen',                 category:'Lab',           subcategory:'Chemistry',   is_cms_shoppable:true  },
  { cpt_code:'84443', description:'Thyroid stimulating hormone TSH',               category:'Lab',           subcategory:'Chemistry',   is_cms_shoppable:true  },
  { cpt_code:'85025', description:'Complete blood count CBC automated',            category:'Lab',           subcategory:'Hematology',  is_cms_shoppable:true  },
  { cpt_code:'86580', description:'TB skin test PPD',                              category:'Lab',           subcategory:'Immunology',  is_cms_shoppable:true  },
  { cpt_code:'86703', description:'HIV-1/2 antibody combination assay',           category:'Lab',           subcategory:'Immunology',  is_cms_shoppable:true  },
  { cpt_code:'87086', description:'Urine culture quantitative',                   category:'Lab',           subcategory:'Microbiology',is_cms_shoppable:true  },
  { cpt_code:'87491', description:'Chlamydia trachomatis amplified probe',        category:'Lab',           subcategory:'Microbiology',is_cms_shoppable:true  },
  { cpt_code:'87804', description:'Influenza rapid test',                          category:'Lab',           subcategory:'Microbiology',is_cms_shoppable:true  },
  // Cardiology
  { cpt_code:'93000', description:'Electrocardiogram ECG 12-lead',                category:'Cardiology',    subcategory:'Diagnostic',  is_cms_shoppable:true  },
  { cpt_code:'93306', description:'Echocardiography with doppler',                category:'Cardiology',    subcategory:'Echo',        is_cms_shoppable:true  },
  { cpt_code:'93350', description:'Stress echocardiography',                      category:'Cardiology',    subcategory:'Echo',        is_cms_shoppable:true  },
  { cpt_code:'93510', description:'Left heart cardiac catheterization',           category:'Cardiology',    subcategory:'Cath Lab',    is_cms_shoppable:true  },
  { cpt_code:'93880', description:'Carotid duplex ultrasound bilateral',          category:'Cardiology',    subcategory:'Vascular',    is_cms_shoppable:true  },
  // GI
  { cpt_code:'43239', description:'Upper GI endoscopy EGD with biopsy',          category:'GI',            subcategory:'Endoscopy',   is_cms_shoppable:true  },
  { cpt_code:'45378', description:'Colonoscopy diagnostic',                        category:'GI',            subcategory:'Colonoscopy', is_cms_shoppable:true  },
  { cpt_code:'45385', description:'Colonoscopy with polyp removal',               category:'GI',            subcategory:'Colonoscopy', is_cms_shoppable:true  },
  // Orthopedics
  { cpt_code:'20610', description:'Arthrocentesis major joint',                   category:'Orthopedics',   subcategory:'Injection',   is_cms_shoppable:true  },
  { cpt_code:'27130', description:'Total hip arthroplasty',                       category:'Orthopedics',   subcategory:'Joint Replacement', is_cms_shoppable:true },
  { cpt_code:'27447', description:'Total knee arthroplasty',                      category:'Orthopedics',   subcategory:'Joint Replacement', is_cms_shoppable:true },
  { cpt_code:'27370', description:'Knee joint injection',                         category:'Orthopedics',   subcategory:'Injection',   is_cms_shoppable:true  },
  { cpt_code:'29827', description:'Arthroscopic rotator cuff repair',             category:'Orthopedics',   subcategory:'Arthroscopy', is_cms_shoppable:true  },
  { cpt_code:'29881', description:'Arthroscopy knee with meniscectomy',           category:'Orthopedics',   subcategory:'Arthroscopy', is_cms_shoppable:true  },
  // Spine
  { cpt_code:'22612', description:'Lumbar spinal fusion',                         category:'Spine',         subcategory:'Surgery',     is_cms_shoppable:true  },
  { cpt_code:'62323', description:'Epidural injection lumbar sacral',             category:'Spine',         subcategory:'Injection',   is_cms_shoppable:true  },
  { cpt_code:'63047', description:'Laminectomy lumbar',                           category:'Spine',         subcategory:'Surgery',     is_cms_shoppable:true  },
  { cpt_code:'64483', description:'Transforaminal epidural injection lumbar',     category:'Spine',         subcategory:'Injection',   is_cms_shoppable:true  },
  // General Surgery
  { cpt_code:'44950', description:'Appendectomy',                                  category:'General Surgery',subcategory:'Appendix',   is_cms_shoppable:true  },
  { cpt_code:'47562', description:'Laparoscopic cholecystectomy',                 category:'General Surgery',subcategory:'Laparoscopic',is_cms_shoppable:true  },
  { cpt_code:'49505', description:'Inguinal hernia repair',                       category:'General Surgery',subcategory:'Hernia',      is_cms_shoppable:true  },
  // OB/GYN
  { cpt_code:'57454', description:'Colposcopy with biopsy',                       category:'OB/GYN',        subcategory:'Diagnostic',  is_cms_shoppable:true  },
  { cpt_code:'58150', description:'Total abdominal hysterectomy',                 category:'OB/GYN',        subcategory:'Surgery',     is_cms_shoppable:true  },
  { cpt_code:'58571', description:'Laparoscopic hysterectomy',                    category:'OB/GYN',        subcategory:'Surgery',     is_cms_shoppable:true  },
  { cpt_code:'59400', description:'Routine obstetric care vaginal delivery',      category:'OB/GYN',        subcategory:'Obstetrics',  is_cms_shoppable:true  },
  { cpt_code:'59510', description:'Routine obstetric care cesarean delivery',     category:'OB/GYN',        subcategory:'Obstetrics',  is_cms_shoppable:true  },
  // Urology
  { cpt_code:'50590', description:'Lithotripsy kidney stones',                    category:'Urology',       subcategory:'Kidney',      is_cms_shoppable:true  },
  { cpt_code:'52000', description:'Cystoscopy',                                   category:'Urology',       subcategory:'Diagnostic',  is_cms_shoppable:true  },
  { cpt_code:'52601', description:'Transurethral resection of prostate TURP',    category:'Urology',       subcategory:'Prostate',    is_cms_shoppable:true  },
  { cpt_code:'55866', description:'Laparoscopic radical prostatectomy',           category:'Urology',       subcategory:'Prostate',    is_cms_shoppable:true  },
  // Primary Care / E&M
  { cpt_code:'99203', description:'Office visit new patient moderate complexity', category:'Primary Care',  subcategory:'Office Visit',is_cms_shoppable:false },
  { cpt_code:'99213', description:'Office visit established patient low',         category:'Primary Care',  subcategory:'Office Visit',is_cms_shoppable:true  },
  { cpt_code:'99214', description:'Office visit established patient moderate',    category:'Primary Care',  subcategory:'Office Visit',is_cms_shoppable:true  },
  { cpt_code:'99283', description:'Emergency department visit moderate severity', category:'Primary Care',  subcategory:'Emergency',   is_cms_shoppable:true  },
  { cpt_code:'99395', description:'Preventive visit established patient 18-39',  category:'Primary Care',  subcategory:'Preventive',  is_cms_shoppable:true  },
  { cpt_code:'99396', description:'Preventive visit established patient 40-64',  category:'Primary Care',  subcategory:'Preventive',  is_cms_shoppable:true  },
  // Mental Health
  { cpt_code:'90791', description:'Psychiatric diagnostic evaluation',            category:'Mental Health', subcategory:'Evaluation',  is_cms_shoppable:true  },
  { cpt_code:'90832', description:'Psychotherapy 30 minutes',                     category:'Mental Health', subcategory:'Therapy',     is_cms_shoppable:true  },
  { cpt_code:'90837', description:'Psychotherapy 60 minutes',                     category:'Mental Health', subcategory:'Therapy',     is_cms_shoppable:true  },
  // Ophthalmology
  { cpt_code:'66984', description:'Cataract removal with lens implant',           category:'Ophthalmology', subcategory:'Surgery',     is_cms_shoppable:true  },
  { cpt_code:'92014', description:'Eye exam established patient comprehensive',   category:'Ophthalmology', subcategory:'Exam',        is_cms_shoppable:true  },
  // ENT
  { cpt_code:'42820', description:'Tonsillectomy and adenoidectomy under 12',     category:'ENT',           subcategory:'Surgery',     is_cms_shoppable:true  },
  { cpt_code:'69436', description:'Tympanostomy ear tubes with general anesthesia',category:'ENT',          subcategory:'Surgery',     is_cms_shoppable:true  },
  // PT/Rehab
  { cpt_code:'97110', description:'Therapeutic exercises',                        category:'Rehab',         subcategory:'PT',          is_cms_shoppable:true  },
  { cpt_code:'97140', description:'Manual therapy techniques',                    category:'Rehab',         subcategory:'PT',          is_cms_shoppable:true  },
  { cpt_code:'97161', description:'Physical therapy evaluation low complexity',   category:'Rehab',         subcategory:'PT',          is_cms_shoppable:true  },
  // HCPCS
  { cpt_code:'G0101', description:'Cervical or vaginal cancer screening',         category:'OB/GYN',        subcategory:'Screening',   is_cms_shoppable:true  },
  { cpt_code:'G0121', description:'Colorectal cancer screening colonoscopy',      category:'GI',            subcategory:'Screening',   is_cms_shoppable:true  },
  { cpt_code:'G0202', description:'Screening mammography',                        category:'Imaging',       subcategory:'Mammography', is_cms_shoppable:true  },
]

async function run() {
  initLog('02_seed_catalog')
  log.info('Enriching procedure_catalog with canonical CMS descriptions...')
  log.info('Note: This runs AFTER Layer 1, which already populated the catalog')
  log.info('from real hospital MRF files. This script adds canonical descriptions')
  log.info('and marks CMS-mandated shoppable services.')

  const client = await pool.connect()
  try {
    for (const p of CATALOG) {
      await client.query(`
        INSERT INTO procedure_catalog (cpt_code, description, category, subcategory, is_cms_shoppable)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (cpt_code) DO UPDATE SET
          description      = EXCLUDED.description,
          category         = EXCLUDED.category,
          subcategory      = EXCLUDED.subcategory,
          is_cms_shoppable = EXCLUDED.is_cms_shoppable
      `, [p.cpt_code, p.description, p.category, p.subcategory, p.is_cms_shoppable])
    }

    const total  = await client.query('SELECT COUNT(*) FROM procedure_catalog')
    const seeded = await client.query('SELECT COUNT(*) FROM procedure_catalog WHERE category IS NOT NULL')
    const shoppable = await client.query('SELECT COUNT(*) FROM procedure_catalog WHERE is_cms_shoppable = true')

    log.ok(`procedure_catalog total: ${total.rows[0].count} procedures`)
    log.ok(`  → ${seeded.rows[0].count} with canonical categories`)
    log.ok(`  → ${shoppable.rows[0].count} CMS-mandated shoppable services`)
    log.ok(`  → ${total.rows[0].count - seeded.rows[0].count} discovered from hospital MRF files`)

  } finally {
    client.release()
    await pool.end()
  }
}
run()
