import { useEffect, useState } from 'react';
import { queryChat } from '../api/client';

export default function ChatPanel({ jobId, selectedNodeId, onHighlights }) {
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [conversationId, setConversationId] = useState('');
  const [messages, setMessages] = useState([]);

  useEffect(() => {
    setConversationId('');
    setMessages([]);
    setError('');
  }, [jobId]);

  const handleSend = async () => {
    const question = input.trim();
    if (!question || loading || !jobId) return;

    setError('');
    setLoading(true);
    setInput('');
    setMessages((prev) => [...prev, { role: 'user', text: question }]);

    try {
      const result = await queryChat({
        jobId,
        question,
        conversationId,
        selectedNodeId,
      });

      if (!conversationId && result.conversation_id) {
        setConversationId(result.conversation_id);
      }

      setMessages((prev) => [
        ...prev,
        {
          role: 'assistant',
          text: result.answer,
          evidence: result.evidence,
          domainAllowed: result.domain_allowed,
        },
      ]);

      const highlights = result.highlights || { node_ids: [], edge_ids: [] };
      onHighlights({
        nodeIds: highlights.node_ids || [],
        edgeIds: highlights.edge_ids || [],
      });
    } catch (e) {
      setError(e.message || 'Chat request failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="side-card chat-card">
      <h3>Chat with Graph</h3>
      <p className="muted">Dataset-grounded answers only</p>

      <div className="chat-messages">
        {messages.length === 0 ? <p className="muted">Ask about entities, relationships, or broken flows.</p> : null}
        {messages.map((msg, idx) => (
          <div key={`${msg.role}-${idx}`} className={`chat-msg ${msg.role}`}>
            <p>{msg.text}</p>
            {msg.role === 'assistant' && msg.evidence ? (
              <div className="chat-evidence">
                <p><strong>Rows:</strong> {msg.evidence.row_count}</p>
                <p><strong>Cypher:</strong> {msg.evidence.cypher || 'n/a'}</p>
                {(msg.evidence.queries || []).map((q, qIdx) => (
                  <p key={`${q.stage}-${qIdx}`}>
                    <strong>{q.stage}:</strong> {q.cypher}
                  </p>
                ))}
              </div>
            ) : null}
          </div>
        ))}
      </div>

      {error ? <p className="error">{error}</p> : null}

      {!jobId ? <p className="muted">Upload and process a dataset first to enable chat.</p> : null}

      <div className="chat-input-row">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Ask about orders, deliveries, invoices, payments..."
          onKeyDown={(e) => {
            if (e.key === 'Enter') handleSend();
          }}
        />
        <button type="button" onClick={handleSend} disabled={loading || !input.trim() || !jobId}>
          {loading ? '...' : 'Send'}
        </button>
      </div>
    </div>
  );
}
