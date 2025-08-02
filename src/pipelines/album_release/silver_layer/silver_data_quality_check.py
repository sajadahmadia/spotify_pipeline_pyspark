


def validate_transformation_results(spark, silver_path):
    """Validate that all tables were created and have expected relationships."""
    validations = []
    
    # Check row counts
    fact_albums_count = spark.read.format('delta').load(f'{silver_path}/fact_albums').count()
    dim_artists_count = spark.read.format('delta').load(f'{silver_path}/dim_artists').count()
    bridge_artists_count = spark.read.format('delta').load(f'{silver_path}/bridge_album_artists').count()
    
    # Validate relationships
    orphaned_artists = spark.sql(f"""
        SELECT COUNT(*) as orphan_count
        FROM delta.`{silver_path}/bridge_album_artists` b
        LEFT JOIN delta.`{silver_path}/dim_artists` d ON b.artist_id = d.artist_id
        WHERE d.artist_id IS NULL
    """).collect()[0]['orphan_count']
    
    if orphaned_artists > 0:
        logger.error(f"Found {orphaned_artists} orphaned artist relationships!")
        
    logger.info(f"Validation Results:")
    logger.info(f"  - Fact Albums: {fact_albums_count} rows")
    logger.info(f"  - Dim Artists: {dim_artists_count} rows")
    logger.info(f"  - Bridge Artists: {bridge_artists_count} rows")
    
    return orphaned_artists == 0