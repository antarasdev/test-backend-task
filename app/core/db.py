from datetime import datetime
from sqlalchemy import Column, String, BigInteger, UUID
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, declared_attr, sessionmaker
from app.core.config import settings

engine = create_async_engine(settings.database_url)

Base = declarative_base()


class Product(Base):
    __tablename__ = 'tables'
    __table_args__ = {'schema': 'product'}  # Указываем схему, если она отличается от стандартной

    classification = Column(String, primary_key=True)
    year = Column(BigInteger)
    period_desc = Column(String)
    aggregate_level = Column(BigInteger)
    trade_flow_code = Column(BigInteger)
    trade_flow = Column(String)
    reporter_code = Column(BigInteger)
    reporter = Column(String)
    partner_code = Column(BigInteger)
    partner = Column(String)
    commodity_code = Column(String)
    commodity = Column(String)
    netweight = Column(BigInteger)
    trade_value = Column(BigInteger)

    def to_dict(self):
        return {
            "classification": self.classification,
            "year": self.year,
            "period_desc": self.period_desc,
            "aggregate_level": self.aggregate_level,
            "trade_flow_code": self.trade_flow_code,
            "trade_flow": self.trade_flow,
            "reporter_code": self.reporter_code,
            "reporter": self.reporter,
            "partner_code": self.partner_code,
            "partner": self.partner,
            "commodity_code": self.commodity_code,
            "commodity": self.commodity,
            "netweight": self.netweight,
            "trade_value": self.trade_value
        }


AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_async_session():
    async with AsyncSessionLocal() as async_session:
        yield async_session
