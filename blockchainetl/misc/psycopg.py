def set_psycopg2_waitable():
    # DONOT set in multithread/process environment
    try:
        import psycopg2
        import psycopg2.extensions
        import psycopg2.extras

        # set wait timeout https://github.com/psycopg/psycopg2/issues/333#issuecomment-543016895
        psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)
    except ImportError:
        pass
