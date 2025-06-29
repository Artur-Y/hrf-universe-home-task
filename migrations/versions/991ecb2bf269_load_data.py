"""Load data

Revision ID: 991ecb2bf269
Revises: 21f6a5adb97e
Create Date: 2023-03-13 10:06:42.751105

"""
import os
import csv

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '991ecb2bf269'
down_revision = '21f6a5adb97e'
branch_labels = None
depends_on = None


def upgrade() -> None:
    dir_name = os.path.dirname(__file__)
    
    # Load standard_job_family
    file_path = os.path.join(dir_name, '../data/standard_job_family.csv')
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip empty rows or rows with missing required fields
            if not row.get('id') or not row.get('id').strip():
                continue
            if not row.get('name') or not row.get('name').strip():
                continue
                
            op.execute(
                sa.text("INSERT INTO public.standard_job_family (id, name) VALUES (:id, :name)").bindparams(
                    sa.bindparam('id', row['id'].strip()),
                    sa.bindparam('name', row['name'].strip())
                )
            )
    
    # Load standard_job
    file_path = os.path.join(dir_name, '../data/standard_job.csv')
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip empty rows or rows with missing required fields
            if not row.get('id') or not row.get('id').strip():
                continue
            if not row.get('name') or not row.get('name').strip():
                continue
            if not row.get('standard_job_family_id') or not row.get('standard_job_family_id').strip():
                continue
                
            op.execute(
                sa.text("INSERT INTO public.standard_job (id, name, standard_job_family_id) VALUES (:id, :name, :family_id)").bindparams(
                    sa.bindparam('id', row['id'].strip()),
                    sa.bindparam('name', row['name'].strip()),
                    sa.bindparam('family_id', row['standard_job_family_id'].strip())
                )
            )
    
    # Load job_posting
    file_path = os.path.join(dir_name, '../data/job_posting.csv')
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip empty rows or rows with missing required fields
            if not row.get('id') or not row.get('id').strip():
                continue
            if not row.get('title') or not row.get('title').strip():
                continue
            if not row.get('standard_job_id') or not row.get('standard_job_id').strip():
                continue
            
            # Handle empty days_to_hire values
            days_to_hire = None
            if row.get('days_to_hire') and row['days_to_hire'].strip():
                try:
                    days_to_hire = int(row['days_to_hire'])
                except ValueError:
                    days_to_hire = None
            
            # Handle empty country_code values
            country_code = row.get('country_code')
            if country_code:
                country_code = country_code.strip() if country_code.strip() else None
            
            op.execute(
                sa.text("""INSERT INTO public.job_posting 
                          (id, title, standard_job_id, country_code, days_to_hire) 
                          VALUES (:id, :title, :job_id, :country_code, :days_to_hire)""").bindparams(
                    sa.bindparam('id', row['id'].strip()),
                    sa.bindparam('title', row['title'].strip()),
                    sa.bindparam('job_id', row['standard_job_id'].strip()),
                    sa.bindparam('country_code', country_code),
                    sa.bindparam('days_to_hire', days_to_hire)
                )
            )


def downgrade() -> None:
    op.execute("DELETE FROM public.job_posting")
    op.execute("DELETE FROM public.standard_job")
    op.execute("DELETE FROM public.standard_job_family")
