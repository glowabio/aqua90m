import aqua90m.utils.exceptions as exc
import logging
logging.TRACE = 5
logging.addLevelName(5, "TRACE")
LOGGER = logging.getLogger(__name__)

def check_outside_europe(lon, lat, LOGGER = None):
    if LOGGER is None:
        LOGGER = logging.getLogger(__name__)

    outside_europe = False
    err_msg = None
    LOGGER.log(logging.TRACE, "CHECKING for in or outside Europe?!")

    if lat > 82:
        err_msg = 'Too far north to be part of Europe: %s' % lat
        outside_europe = True
    elif lat < 34:
        err_msg = 'Too far south to be part of Europe: %s' % lat
        outside_europe = True
    if lon > 70:
        err_msg = 'Too far east to be part of Europe: %s' % lon
        outside_europe = True
    elif lon < -32:
        err_msg = 'Too far west to be part of Europe: %s' % lon
        outside_europe = True

    if outside_europe:
        LOGGER.error(err_msg)
        raise exc.OutsideAreaException(err_msg)

    return False

if __name__ == "__main__":

    # Logging
    verbose = True
    #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)5s - %(message)s')
    logging.basicConfig(level=logging.DEBUG, format='%(name)s:%(lineno)s - %(levelname)5s - %(message)s')
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    lon = 9.931555
    lat = 54.695070
    print('\nSTART RUNNING FUNCTION: check_outside_europe')
    res = check_outside_europe(lon, lat, LOGGER)
    print('RESULT: %s' % res)
    print('\nSTART RUNNING FUNCTION: check_outside_europe')
    res = check_outside_europe(lon, lat)
    print('RESULT: %s' % res)

    lon = 200
    lat = 10
    print('\nSTART RUNNING FUNCTION: check_outside_europe')
    try:
        res = check_outside_europe(lon, lat, LOGGER)
        print('RESULT: %s' % res)
    except ValueError as e:
        print('RESULT: %s' % e)
