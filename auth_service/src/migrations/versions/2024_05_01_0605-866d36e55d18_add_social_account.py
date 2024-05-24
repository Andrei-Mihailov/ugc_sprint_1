"""Add social account

Revision ID: 866d36e55d18
Revises: b9e284d66a5a
Create Date: 2024-05-01 06:05:10.180023

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from db.postgres_db import Base


# revision identifiers, used by Alembic.
revision: str = '866d36e55d18'
down_revision: Union[str, None] = 'b9e284d66a5a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('social_account',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('user_id', sa.UUID(), nullable=False),
    sa.Column('social_id', sa.Text(), nullable=False),
    sa.Column('social_name', sa.Text(), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('social_id', 'social_name', name='social_pk')
    )
    op.drop_constraint('authentication_user_id_fkey', 'authentication', type_='foreignkey')
    op.create_foreign_key(None, 'authentication', 'users', ['user_id'], ['id'], ondelete='CASCADE')
    op.drop_constraint('permissions_role_id_fkey', 'permissions', type_='foreignkey')
    op.create_foreign_key(None, 'permissions', 'roles', ['role_id'], ['id'], ondelete='CASCADE')
    op.drop_constraint('users_role_id_fkey', 'users', type_='foreignkey')
    op.create_foreign_key(None, 'users', 'roles', ['role_id'], ['id'], ondelete='CASCADE')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'users', type_='foreignkey')
    op.create_foreign_key('users_role_id_fkey', 'users', 'roles', ['role_id'], ['id'])
    op.drop_constraint(None, 'permissions', type_='foreignkey')
    op.create_foreign_key('permissions_role_id_fkey', 'permissions', 'roles', ['role_id'], ['id'])
    op.drop_constraint(None, 'authentication', type_='foreignkey')
    op.create_foreign_key('authentication_user_id_fkey', 'authentication', 'users', ['user_id'], ['id'])
    op.drop_table('social_account')
    # ### end Alembic commands ###