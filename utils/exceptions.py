

class Aqua90mException(Exception):
    pass

class UserInputException(Aqua90mException):
    pass

class OutsideAreaException(UserInputException):
    # TODO Maybe only UserInputException?
    pass

class DataAccessException(Aqua90mException):
    # TODO Maybe only UserInputException?
    pass

class GeoFreshTooManySubcatchments(Aqua90mException):
    pass

class GeoFreshNoResultException(Aqua90mException):
    # TODO: This may actually be control flow and should be handled by code, not
    # by an exception
    pass

class GeoFreshUnexpectedResultException(Aqua90mException):
    pass
