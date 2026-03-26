import { useEffect, useMemo, useState } from 'react';
import { getGraph, getJob, uploadZip } from './api/client';
import GraphView from './components/GraphView';
import StatusPanel from './components/StatusPanel';
import UploadPanel from './components/UploadPanel';

const terminalStates = new Set(['completed', 'failed']);

export default function App() {
  const [jobId, setJobId] = useState('');
  const [jobState, setJobState] = useState(null);
  const [graph, setGraph] = useState({ nodes: [], edges: [] });
  const [selectedNode, setSelectedNode] = useState(null);
  const [chatHighlights, setChatHighlights] = useState({ nodeIds: [], edgeIds: [] });
  const [error, setError] = useState('');
  const [uploading, setUploading] = useState(false);

  useEffect(() => {
    let timer;

    async function poll() {
      if (!jobId) return;
      try {
        const state = await getJob(jobId);
        setJobState(state);

        if (terminalStates.has(state.status)) {
          if (state.status === 'completed') {
            const graphResponse = await getGraph('granular', jobId);
            setGraph({
              nodes: graphResponse.nodes || [],
              edges: graphResponse.edges || [],
            });
            setSelectedNode(null);
            setChatHighlights({ nodeIds: [], edgeIds: [] });
          }
          setUploading(false);
          return;
        }
      } catch (e) {
        setUploading(false);
        setError(e.message || 'Failed to poll job state');
        return;
      }

      timer = setTimeout(poll, 2000);
    }

    poll();
    return () => timer && clearTimeout(timer);
  }, [jobId]);

  const graphStats = useMemo(() => {
    return {
      nodes: graph.nodes?.length || 0,
      edges: graph.edges?.length || 0,
    };
  }, [graph]);

  const onUpload = async (file) => {
    setError('');
    setUploading(true);
    setGraph({ nodes: [], edges: [] });
    setChatHighlights({ nodeIds: [], edgeIds: [] });
    try {
      const result = await uploadZip(file);
      setJobId(result.job_id);
    } catch (e) {
      setUploading(false);
      setError(e.message || 'Upload failed');
    }
  };

  return (
    <div className="app-root">
      <header className="hero">
        <h1>Graph-Based Data Modeling</h1>
        <p>Upload dataset ZIP, infer table links, and view the operational flow map.</p>
      </header>

      <section className="top-grid">
        <UploadPanel onUpload={onUpload} disabled={uploading} />
        <StatusPanel state={jobState} error={error} />
      </section>

      <section className="graph-header">
        <h2>Relationship Flow</h2>
        <p>{graphStats.nodes} nodes · {graphStats.edges} edges</p>
      </section>

      <GraphView
        jobId={jobId}
        graph={graph}
        selectedNode={selectedNode}
        onSelectNode={setSelectedNode}
        chatHighlights={chatHighlights}
        onChatHighlights={setChatHighlights}
      />
    </div>
  );
}
