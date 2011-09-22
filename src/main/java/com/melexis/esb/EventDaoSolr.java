package com.melexis.esb;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EventDaoSolr implements EventDao {

    private final SolrServer solr;

    public EventDaoSolr(SolrServer solr) {
        this.solr = solr;
    }

    @Override
    public void store(Event event) {
        try {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("timestamp", event.getTimestamp().toDate());
            doc.addField("source", event.getSource());

            for (final Map.Entry<String, String> e: event.getAttributes().entrySet()) {
                doc.addField("attribute." + e.getKey(), e.getValue());
            }

            solr.add(doc);
            solr.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Event> findEvents(String source, @Nullable DateTime from, @Nullable DateTime till, int max) {
        SolrQuery q = new SolrQuery();
        q.setQuery("source:" + source);
        q.setRows(max);
        q.addSortField("timestamp", SolrQuery.ORDER.asc);

        QueryResponse qr = null;
        try {
            qr = solr.query(q);
            final List<Event> events = new ArrayList<Event>();

            while (qr.getResults().iterator().hasNext()) {
                SolrDocument doc = qr.getResults().iterator().next();
                events.add(toEvent(doc));
            }

            return events;
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        }
    }

    public static Event toEvent(SolrDocument doc) {
        Pattern p = Pattern.compile("attribute\\.(\\w*)");
        Map<String, String> attributes = new HashMap<String, String>();
        for (final String key: doc.getFieldNames()) {
            Matcher m = p.matcher(key);
            if (m.matches()) {
                attributes.put(m.group(1), doc.getFieldValue(key).toString());
            }

        }

        return Event.createEvent(new DateTime(doc.getFieldValue("timestamp")),
                doc.getFieldValue("source").toString(), attributes);
    }
}
