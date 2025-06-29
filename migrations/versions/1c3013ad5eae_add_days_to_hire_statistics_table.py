"""Add days_to_hire_statistics table

Revision ID: 1c3013ad5eae
Revises: 991ecb2bf269
Create Date: 2025-06-28 18:00:30.375984

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1c3013ad5eae'
down_revision = '991ecb2bf269'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create days_to_hire_statistics table
    op.create_table(
        'days_to_hire_statistics',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('standard_job_id', sa.String(), nullable=False),
        sa.Column('country_code', sa.String(), nullable=True),
        sa.Column('min_days', sa.Float(), nullable=False),
        sa.Column('avg_days', sa.Float(), nullable=False),
        sa.Column('max_days', sa.Float(), nullable=False),
        sa.Column('job_postings_number', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        schema='public'
    )


def downgrade() -> None:
    # Drop days_to_hire_statistics table
    op.drop_table('days_to_hire_statistics', schema='public')
