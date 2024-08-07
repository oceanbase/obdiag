class ObdiagResult:
    # ObdiagResult is the result of obdiag.
    # It contains the code and result of obdiag.

    # SERVER_ERROR_CODE(5xx) is the code of server error.
    SERVER_ERROR_CODE = 500
    # INPUT_ERROR_CODE(4xx) is the code of input error.
    INPUT_ERROR_CODE = 400
    # SUCCESS_CODE(200) is the code of success.
    SUCCESS_CODE = 200

    def __init__(self, code, data=None, error_data=None):
        self.trace_id = None
        self.data = data
        self.error_data = error_data
        if code is None:
            raise TypeError("ObdiagResult code is None. Please contact the Oceanbase community. ")
        if isinstance(code, int):
            self.code = str(code)
        else:
            raise TypeError("ObdiagResult code is not int. Please contact the Oceanbase community. ")
        if isinstance(data, dict):
            self.result = data
        else:
            raise TypeError("ObdiagResult result is not str. Please contact the Oceanbase community. ")
        if isinstance(error_data, str):
            self.error_data = error_data
        else:
            raise TypeError("ObdiagResult error_data is not str. Please contact the Oceanbase community. ")

    def set_trace_id(self, trace_id):
        if trace_id is None:
            raise TypeError("ObdiagResult trace_id is None. Please contact the Oceanbase community. ")
        if isinstance(trace_id, str):
            self.trace_id = trace_id
        else:
            raise TypeError("ObdiagResult trace_id is not str. Please contact the Oceanbase community. ")
