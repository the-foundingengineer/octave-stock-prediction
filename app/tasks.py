# app/tasks.py

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.news_service import update_stock_news
import asyncio
import logging

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

STOCKS = [
    "AIRTELAFRI",
    "BUAFOODS",
    "MTNN",
    "DANGCEM",
    "BUACEMENT",
    "SEPLAT",
    "ARADEL",
    "GTCO",
    "ZENITHBANK",
    "GEREGU",
    "WAPCO",
    "INTBREW",
    "NB",
    "TRANSPOWER",
    "FIRSTHOLDCO",
    "PRESCO",
    "STANBIC",
    "UBA",
    "TRANSCOHOT",
    "NESTLE",
    "ACCESSCORP",
    "OKOMUOIL",
    "ETI",
    "WEMABANK",
    "FIDELITYBK",
    "DANGSUGAR",
    "GUINNESS",
    "FCMB",
    "OANDO",
    "TRANSCORP",
    "UNILEVER",
    "MECURE",
    "STERLINGNG",
    "JAIZBANK",
    "UCAP",
    "NASCON",
    "JBERGER",
    "CUSTODIAN",
    "UACN",
    "BETAGLAS",
    "NAHCO",
    "PZ",
    "TOTAL",
    "NGXGROUP",
    "FIDSON",
    "SKYAVN",
    "ABBEYBDS",
    "HONYFLOUR",
    "CHAMPION",
    "ETRANZACT",
    "NEM",
    "AIICO",
    "CADBURY",
    "VITAFOAM",
    "MANSARD",
    "CONOIL",
    "CORNERST",
    "UPDC",
    "VFDGROUP",
    "MBENEFIT",
    "WAPIC",
    "IKEJAHOTEL",
    "CAP",
    "ETERNA",
    "MAYBAKER",
    "CWG",
    "AFRIPRUD",
    "ELLAHLAKES",
    "LINKASSURE",
    "CONHALLPLC",
    "SOVRENINS",
    "NEIMETH",
    "CHAMS",
    "INFINITY",
    "NPFMCRFBK",
    "EUNISELL",
    "JAPAULGOLD",
    "VERITASKAP",
    "FTNCOCOA",
    "LASACO",
    "UNIVINSURE",
    "IMG",
    "SUNUASSUR",
    "CUTIX",
    "LIVINGTRUST",
    "CAVERTON",
    "CILEASING",
    "LIVESTOCK",
    "NCR",
    "PRESTIGE",
    "UPDCREIT",
    "SCOA",
    "TANTALIZER",
    "BERGER",
    "TIP",
    "REGALINS",
    "UNITYBNK",
    "UHOMREIT",
    "GUINEAINS",
    "ROYALEX",
    "REDSTAREX",
    "MORISON",
    "DAARCOMM",
    "NNFM",
    "ABCTRANS",
    "RTBRISCOE",
    "MULTIVERSE",
    "DEAPCAP",
    "MEYER",
    "CHELLARAM",
    "SFSREIT",
    "LEARNAFRCA",
    "ACADEMY",
    "MCNICHOLS",
    "TRIPPLEG",
    "GOLDBREW",
    "OMATEK",
    "NSLTECH",
    "AUSTINLAZ",
    "STACO",
    "AFRINSURE",
    "INTENEGINS",
    "ALEX",
    "ENAMELWA",
    "UNIONDICON",
    "JOHNHOLT",
    "FTGINSURE",
    "UPL",
    "PREMPAINTS",
    "MULTITREX",
    "TRANSEXPR",
    "AFROMEDIA",
    "PHARMDEKO",
    "MOFIREIF",
    "ZICHIS",
    "EKOCORP",
    "CNIF",
    "NIDF",
    "NREIT",
    "JULI",
    "HMCALL",
    "VANLEER",
    "THOMASWY",
    "LEGENDINT"
]

async def update_all_stocks():
    logger.info("Starting news update job...")

    from app.models import Stock  # local import to avoid circular imports

    db: Session = SessionLocal()

    try:
        # Fetch all Stock rows whose symbol is in our watchlist
        stocks = db.query(Stock).filter(Stock.symbol.in_(STOCKS)).all()

        if not stocks:
            logger.warning("No matching stocks found in DB — skipping news update.")
            return

        # Run all news fetches concurrently
        job_tasks = [update_stock_news(db, stock) for stock in stocks]
        await asyncio.gather(*job_tasks)

        logger.info(f"Finished updating news for {len(stocks)} stocks.")

    except Exception as e:
        logger.error(f"Error in scheduler job: {e}")

    finally:
        db.close()



def start_scheduler():
    scheduler.add_job(
        update_all_stocks,
        trigger="interval",
        minutes=15,   # adjust as needed
        max_instances=1  # prevents overlapping jobs
    )
    scheduler.start()