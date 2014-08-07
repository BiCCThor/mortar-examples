@outputSchema('us_state:chararray')
def us_state(full_place):
    """
    Parse the full place name (e.g. Wheeling, WV) to the state name (WV).
    """
    # convert input to a string and find the last comma in the string
    full_place_text = full_place.tostring()
    last_comma_idx = full_place_text.rfind(',')
    if last_comma_idx > 0:
        # grab just the state name
        state = full_place_text[last_comma_idx+1:].strip() 
        return state
    else:
        return None
