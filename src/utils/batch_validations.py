
def column_between(batch, column_name, min_value=0, max_value=None):
    """Verify min column value

    Args:
        batch (batch_ge): Batch great expectations df
        column_name (str): Column to validate
        min_value (int, optional): Min value to validate. Defaults to 0.

    Returns:
        dict: Dict with expectation result
    """
    batch_result = batch.expect_column_values_to_be_between(column_name, min_value=min_value,
        result_format={'result_format': 'COMPLETE'},
        include_config=True)

    return batch_result

def column_not_be_null(batch, column_name):
    """Verify wich column have null values

    Args:
        batch (batch_ge): Batch great expectations df
        column_name (str): Column to validate
        semantical_missing (str, optional): Semantical missing value to verify if is null, Defaults to "". 

    Returns:
        dict: Dict with expectation result
    """
    batch_result = batch.expect_column_values_to_not_be_null(column_name)

    return batch_result


def column_match_regex(batch, column_name, pattern):
    import re
    """Verify wich values don't mach with a regex pattern

    Args:
        batch (batch_ge): Batch great expectations df
        column_name (str): Column to validate
        pattern (str): pattern regex value

    Returns:
        dict: Dict with expectation result
    """
    batch_result = batch.expect_column_values_to_match_regex(column_name,
        pattern,
        result_format={'result_format': 'COMPLETE'})

    return batch_result