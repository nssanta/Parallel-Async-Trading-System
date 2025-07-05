from decimal import Decimal, ROUND_DOWN, InvalidOperation

def round_decimal(value, decimals):
    """
    Rounds a number to a specified number of decimal places.
    :param value: The number to round.
    :param decimals: Number of decimal places.
    :return: Rounded Decimal value.
    """
    if decimals < 0:
        raise ValueError("Decimal places must be non-negative.")

    try:
        value_decimal = Decimal(str(value))  # Convert to string for safe Decimal conversion
    except (ValueError, InvalidOperation) as e:
        raise ValueError(f"Invalid value for rounding: {value}") from e

    # Create rounding factor (e.g., 1.00 for 2 decimals)
    rounding_factor = Decimal('1.' + '0' * int(decimals))

    try:
        rounded_value = value_decimal.quantize(rounding_factor, rounding=ROUND_DOWN)
    except InvalidOperation as e:
        raise ValueError(f"Rounding error for value: {value_decimal}") from e

    return rounded_value

def get_decimal_places(price):
    """
    Returns the number of decimal places in a price.
    :param price: Price value.
    :return: Number of decimal places (int).
    """
    price_str = str(price)
    if '.' in price_str:
        return len(price_str.split('.')[1])
    return 1
