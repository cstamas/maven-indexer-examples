package org.apache.maven.indexer.example;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.maven.index.ArtifactInfo;
import org.apache.maven.index.ArtifactInfoFilter;
import org.apache.maven.index.ArtifactInfoGroup;
import org.apache.maven.index.Field;
import org.apache.maven.index.FlatSearchRequest;
import org.apache.maven.index.FlatSearchResponse;
import org.apache.maven.index.GroupedSearchRequest;
import org.apache.maven.index.GroupedSearchResponse;
import org.apache.maven.index.Grouping;
import org.apache.maven.index.Indexer;
import org.apache.maven.index.IteratorSearchRequest;
import org.apache.maven.index.IteratorSearchResponse;
import org.apache.maven.index.MAVEN;
import org.apache.maven.index.context.IndexCreator;
import org.apache.maven.index.context.IndexUtils;
import org.apache.maven.index.context.IndexingContext;
import org.apache.maven.index.expr.SourcedSearchExpression;
import org.apache.maven.index.expr.UserInputSearchExpression;
import org.apache.maven.index.search.grouping.GAGrouping;
import org.apache.maven.index.updater.IndexUpdateRequest;
import org.apache.maven.index.updater.IndexUpdateResult;
import org.apache.maven.index.updater.IndexUpdater;
import org.apache.maven.index.updater.ResourceFetcher;
import org.apache.maven.index.updater.WagonHelper;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.events.TransferListener;
import org.apache.maven.wagon.observers.AbstractTransferListener;
import org.codehaus.plexus.DefaultPlexusContainer;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.PlexusContainerException;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.codehaus.plexus.util.StringUtils;
import org.sonatype.aether.util.version.GenericVersionScheme;
import org.sonatype.aether.version.InvalidVersionSpecificationException;
import org.sonatype.aether.version.Version;

/**
 * Hello world!
 */
public class App
{
    public static void main( String[] args )
        throws Exception
    {
        final App app = new App();
        app.perform();
    }

    // ==

    private final PlexusContainer plexusContainer;

    private final Indexer indexer;

    private final IndexUpdater indexUpdater;

    private final Wagon httpWagon;

    public App()
        throws PlexusContainerException, ComponentLookupException
    {
        // here we create Plexus container, the Maven default IoC container
        // Plexus falls outside of MI scope, just accept the fact that
        // MI is a Plexus component ;)
        // If needed more info, ask on Maven Users list or Plexus Users list
        // google is your friend!
        this.plexusContainer = new DefaultPlexusContainer();

        // lookup the indexer components from plexus
        this.indexer = plexusContainer.lookup( Indexer.class );
        this.indexUpdater = plexusContainer.lookup( IndexUpdater.class );
        // lookup wagon used to remotely fetch index
        this.httpWagon = plexusContainer.lookup( Wagon.class, "http" );

    }

    public void perform()
        throws IOException, ComponentLookupException, InvalidVersionSpecificationException
    {
        // Files where local cache is (if any) and Lucene Index should be located
        File centralLocalCache = new File( "target/central-cache" );
        File centralIndexDir = new File( "target/central-index" );

        // Creators we want to use (search for fields it defines)
        List<IndexCreator> indexers = new ArrayList<IndexCreator>();
        indexers.add( plexusContainer.lookup( IndexCreator.class, "min" ) );
        indexers.add( plexusContainer.lookup( IndexCreator.class, "jarContent" ) );
        indexers.add( plexusContainer.lookup( IndexCreator.class, "maven-plugin" ) );

        // Create context for central repository index
        IndexingContext centralContext =
            indexer.createIndexingContext( "central-context", "central", centralLocalCache, centralIndexDir,
                                           "http://repo1.maven.org/maven2", null, true, true, indexers );

        // Update the index (incremental update will happen if this is not 1st run and files are not deleted)
        // This whole block below should not be executed on every app start, but rather controlled by some configuration
        // since this block will always emit at least one HTTP GET. Central indexes are updated once a week, but
        // other index sources might have different index publishing frequency.
        // Preferred frequency is once a week.
        if ( true )
        {
            System.out.println( "Updating Index..." );
            System.out.println( "This might take a while on first run, so please be patient!" );
            // Create ResourceFetcher implementation to be used with IndexUpdateRequest
            // Here, we use Wagon based one as shorthand, but all we need is a ResourceFetcher implementation
            TransferListener listener = new AbstractTransferListener()
            {
                public void transferStarted( TransferEvent transferEvent )
                {
                    System.out.print( "  Downloading " + transferEvent.getResource().getName() );
                }

                public void transferProgress( TransferEvent transferEvent, byte[] buffer, int length )
                {
                }

                public void transferCompleted( TransferEvent transferEvent )
                {
                    System.out.println( " - Done" );
                }
            };
            ResourceFetcher resourceFetcher = new WagonHelper.WagonFetcher( httpWagon, listener, null, null );

            Date centralContextCurrentTimestamp = centralContext.getTimestamp();
            IndexUpdateRequest updateRequest = new IndexUpdateRequest( centralContext, resourceFetcher );
            IndexUpdateResult updateResult = indexUpdater.fetchAndUpdateIndex( updateRequest );
            if ( updateResult.isFullUpdate() )
            {
                System.out.println( "Full update happened!" );
            }
            else if ( updateResult.getTimestamp().equals( centralContextCurrentTimestamp ) )
            {
                System.out.println( "No update needed, index is up to date!" );
            }
            else
            {
                System.out.println( "Incremental update happened, change covered " + centralContextCurrentTimestamp
                                        + " - " + updateResult.getTimestamp() + " period." );
            }

            System.out.println();
        }

        System.out.println();
        System.out.println( "Using index" );
        System.out.println( "===========" );
        System.out.println();

        // ====
        // Case:
        // dump all the GAVs
        // NOTE: will not actually execute do this below, is too long to do (Central is HUGE), but is here as code example
        if ( false )
        {
            final IndexSearcher searcher = centralContext.acquireIndexSearcher();
            try
            {
                final IndexReader ir = searcher.getIndexReader();
                for ( int i = 0; i < ir.maxDoc(); i++ )
                {
                    if ( !ir.isDeleted( i ) )
                    {
                        final Document doc = ir.document( i );
                        final ArtifactInfo ai = IndexUtils.constructArtifactInfo( doc, centralContext );
                        System.out.println( ai.groupId + ":" + ai.artifactId + ":" + ai.version + ":" + ai.classifier
                                                + " (sha1=" + ai.sha1 + ")" );
                    }
                }
            }
            finally
            {
                centralContext.releaseIndexSearcher( searcher );
            }
        }

        // ====
        // Case:
        // Search for all GAVs with known G and A and having version greater than V

        final GenericVersionScheme versionScheme = new GenericVersionScheme();
        final String versionString = "1.5.0";
        final Version version = versionScheme.parseVersion( versionString );

        // construct the query for known GA
        final Query groupIdQ =
            indexer.constructQuery( MAVEN.GROUP_ID, new SourcedSearchExpression( "org.sonatype.nexus" ) );
        final Query artifactIdQ =
            indexer.constructQuery( MAVEN.ARTIFACT_ID, new SourcedSearchExpression( "nexus-api" ) );
        final BooleanQuery query = new BooleanQuery();
        query.add( groupIdQ, Occur.MUST );
        query.add( artifactIdQ, Occur.MUST );

        // we want "jar" artifacts only
        query.add( indexer.constructQuery( MAVEN.PACKAGING, new SourcedSearchExpression( "jar" ) ), Occur.MUST );
        // we want main artifacts only (no classifier)
        // Note: this below is unfinished API, needs fixing
        query.add( indexer.constructQuery( MAVEN.CLASSIFIER, new SourcedSearchExpression( Field.NOT_PRESENT ) ),
                   Occur.MUST_NOT );

        // construct the filter to express "V greater than"
        final ArtifactInfoFilter versionFilter = new ArtifactInfoFilter()
        {
            public boolean accepts( final IndexingContext ctx, final ArtifactInfo ai )
            {
                try
                {
                    final Version aiV = versionScheme.parseVersion( ai.version );
                    // Use ">=" if you are INCLUSIVE
                    return aiV.compareTo( version ) > 0;
                }
                catch ( InvalidVersionSpecificationException e )
                {
                    // do something here? be safe and include?
                    return true;
                }
            }
        };

        final IteratorSearchRequest request = new IteratorSearchRequest( query, versionFilter );
        final IteratorSearchResponse response = indexer.searchIterator( request );
        for ( ArtifactInfo ai : response )
        {
            System.out.println( ai.toString() );
        }

        // Case:
        // Use index
        // Searching for some artifact
        Query gidQ =
            indexer.constructQuery( MAVEN.GROUP_ID, new SourcedSearchExpression( "org.apache.maven.indexer" ) );
        Query aidQ =
            indexer.constructQuery( MAVEN.ARTIFACT_ID, new SourcedSearchExpression( "indexer-artifact" ) );

        BooleanQuery bq = new BooleanQuery();
        bq.add( gidQ, Occur.MUST );
        bq.add( aidQ, Occur.MUST );

        searchAndDump( indexer, "all artifacts under GA org.apache.maven.indexer:indexer-artifact", bq );

        // Searching for some main artifact
        bq = new BooleanQuery();
        bq.add( gidQ, Occur.MUST );
        bq.add( aidQ, Occur.MUST );
        // bq.add( nexusIndexer.constructQuery( MAVEN.CLASSIFIER, new SourcedSearchExpression( "*" ) ), Occur.MUST_NOT
        // );

        searchAndDump( indexer, "main artifacts under GA org.apache.maven.indexer:indexer-artifact", bq );

        // doing sha1 search
        searchAndDump( indexer, "SHA1 7ab67e6b20e5332a7fb4fdf2f019aec4275846c2", indexer.constructQuery(
            MAVEN.SHA1, new SourcedSearchExpression( "7ab67e6b20e5332a7fb4fdf2f019aec4275846c2" ) ) );

        searchAndDump( indexer, "SHA1 7ab67e6b20 (partial hash)",
                       indexer.constructQuery( MAVEN.SHA1, new UserInputSearchExpression( "7ab67e6b20" ) ) );

        // doing classname search (incomplete classname)
        searchAndDump( indexer,
                       "classname DefaultNexusIndexer (note: Central does not publish classes in the index)",
                       indexer.constructQuery( MAVEN.CLASSNAMES,
                                               new UserInputSearchExpression( "DefaultNexusIndexer" ) ) );

        // doing search for all "canonical" maven plugins latest versions
        bq = new BooleanQuery();
        bq.add( indexer.constructQuery( MAVEN.PACKAGING, new SourcedSearchExpression( "maven-plugin" ) ),
                Occur.MUST );
        bq.add(
            indexer.constructQuery( MAVEN.GROUP_ID, new SourcedSearchExpression( "org.apache.maven.plugins" ) ),
            Occur.MUST );
        searchGroupedAndDump( indexer, "all \"canonical\" maven plugins", bq, new GAGrouping() );

        // close cleanly
        indexer.closeIndexingContext( centralContext, false );
    }

    public static void searchAndDump( Indexer nexusIndexer, String descr, Query q )
        throws IOException
    {
        System.out.println( "Searching for " + descr );

        FlatSearchResponse response = nexusIndexer.searchFlat( new FlatSearchRequest( q ) );

        for ( ArtifactInfo ai : response.getResults() )
        {
            System.out.println( ai.toString() );
        }

        System.out.println( "------" );
        System.out.println( "Total: " + response.getTotalHitsCount() );
        System.out.println();
    }

    public static void searchGroupedAndDump( Indexer nexusIndexer, String descr, Query q, Grouping g )
        throws IOException
    {
        System.out.println( "Searching for " + descr );

        GroupedSearchResponse response = nexusIndexer.searchGrouped( new GroupedSearchRequest( q, g ) );

        for ( Map.Entry<String, ArtifactInfoGroup> entry : response.getResults().entrySet() )
        {
            ArtifactInfo ai = entry.getValue().getArtifactInfos().iterator().next();
            System.out.println( "* Plugin " + ai.artifactId );
            System.out.println( "  Latest version:  " + ai.version );
            System.out.println( StringUtils.isBlank( ai.description ) ? "No description in plugin's POM."
                                    : StringUtils.abbreviate( ai.description, 60 ) );
            System.out.println();
        }

        System.out.println( "------" );
        System.out.println( "Total record hits: " + response.getTotalHitsCount() );
        System.out.println();
    }
}
