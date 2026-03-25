import { useEffect, useMemo, useRef, useState } from 'react';
import Graph from 'graphology';
import Sigma from 'sigma';

const TABLE_COLORS = [
  '#5ca8ff',
  '#7fb7ff',
  '#9cc6ff',
  '#f29bab',
  '#f6a9b6',
  '#f8b5c1',
  '#89b4fa',
  '#b8cdfa',
  '#e9a8bf',
  '#c2d4ff',
];

function colorForTable(name) {
  let hash = 0;
  for (let i = 0; i < name.length; i += 1) {
    hash = (hash << 5) - hash + name.charCodeAt(i);
    hash |= 0;
  }
  const index = Math.abs(hash) % TABLE_COLORS.length;
  return TABLE_COLORS[index];
}

function mapNodes(rawNodes, rawEdges) {
  const byTable = new Map();
  rawNodes.forEach((node) => {
    const table = node.data?.table || node.data?.entity || 'unknown';
    if (!byTable.has(table)) {
      byTable.set(table, []);
    }
    byTable.get(table).push(node);
  });

  const tables = [...byTable.keys()].sort();
  const degree = new Map();
  rawEdges.forEach((edge) => {
    degree.set(edge.source, (degree.get(edge.source) || 0) + 1);
    degree.set(edge.target, (degree.get(edge.target) || 0) + 1);
  });

  const centerRadius = Math.max(2600, tables.length * 320);
  const ellipseRatio = 1.04;

  const output = [];
  tables.forEach((table, tableIndex) => {
    const bucket = byTable.get(table) || [];
    const theta = (2 * Math.PI * tableIndex) / Math.max(tables.length, 1);
    const cx = Math.cos(theta) * centerRadius;
    const cy = Math.sin(theta) * (centerRadius * ellipseRatio);
    const perRing = 9;
    const baseClusterRadius = 460 + Math.min(620, bucket.length * 1.6);

    bucket.forEach((node, idx) => {
      const ring = Math.floor(idx / perRing) + 1;
      const inRing = idx % perRing;
      const step = (2 * Math.PI) / perRing;
      const angle = inRing * step;
      const jitter = (idx % 15) * 8.0;
      const radius = baseClusterRadius + (ring - 1) * 300 + jitter;
      const x = cx + Math.cos(angle) * radius;
      const y = cy + Math.sin(angle) * radius;

      const deg = degree.get(node.id) || 0;
      const size = Math.max(5, Math.min(12, 5 + Math.log2(deg + 1) * 2));
      const fill = colorForTable(table);

      output.push({
        id: node.id,
        position: { x, y },
        data: {
          label: node.label,
          table,
          fields: node.data?.fields || {},
          rowId: node.data?.row_id || node.label,
          fill,
          size,
          degree: deg,
        },
      });
    });
  });

  return output;
}

function mapEdges(rawEdges) {
  return rawEdges.map((edge, idx) => {
    const isNested = edge.data?.edge_type === 'NESTED_PARENT_CHILD';
    return {
      id: edge.id || `edge-${idx}`,
      source: edge.source,
      target: edge.target,
      isNested,
      data: edge.data || {},
    };
  });
}

function buildAdjacency(allEdges) {
  const neighbors = new Map();
  const incidentEdges = new Map();

  const ensure = (id) => {
    if (!neighbors.has(id)) neighbors.set(id, new Set());
    if (!incidentEdges.has(id)) incidentEdges.set(id, new Set());
  };

  allEdges.forEach((edge) => {
    ensure(edge.source);
    ensure(edge.target);
    neighbors.get(edge.source).add(edge.target);
    neighbors.get(edge.target).add(edge.source);
    incidentEdges.get(edge.source).add(edge.id);
    incidentEdges.get(edge.target).add(edge.id);
  });

  return { neighbors, incidentEdges };
}

function FieldList({ fields }) {
  const entries = Object.entries(fields || {});
  if (!entries.length) {
    return <p className="inspector-empty">No field data available.</p>;
  }

  return (
    <div className="field-list">
      {entries.map(([k, v]) => (
        <p key={k}>
          <strong>{k}:</strong> {v === null || v === undefined ? 'null' : String(v)}
        </p>
      ))}
    </div>
  );
}

export default function GraphView({ graph, selectedNode, onSelectNode }) {
  const [zoomLevel, setZoomLevel] = useState(1);
  const [isFocusLocked, setIsFocusLocked] = useState(false);
  const [lockedFocusNode, setLockedFocusNode] = useState(null);
  const containerRef = useRef(null);
  const sigmaRef = useRef(null);
  const graphRef = useRef(null);
  const selectedNodeRef = useRef(selectedNode);
  const isFocusLockedRef = useRef(isFocusLocked);

  const allEdges = useMemo(() => mapEdges(graph?.edges || []), [graph]);
  const nodes = useMemo(() => mapNodes(graph?.nodes || [], graph?.edges || []), [graph]);
  const adjacency = useMemo(() => buildAdjacency(allEdges), [allEdges]);

  const selectedDetails = useMemo(() => {
    if (!selectedNode) {
      return null;
    }
    return nodes.find((n) => n.id === selectedNode)?.data || null;
  }, [nodes, selectedNode]);

  const edgeStats = useMemo(() => {
    const stats = { data: 0, nested: 0 };
    (graph?.edges || []).forEach((edge) => {
      if (edge.data?.edge_type === 'NESTED_PARENT_CHILD') {
        stats.nested += 1;
      } else {
        stats.data += 1;
      }
    });
    return stats;
  }, [graph]);

  useEffect(() => {
    selectedNodeRef.current = selectedNode;
  }, [selectedNode]);

  useEffect(() => {
    isFocusLockedRef.current = isFocusLocked;
  }, [isFocusLocked]);

  const handleUnlockFocus = () => {
    setIsFocusLocked(false);
    setLockedFocusNode(null);
  };

  const handleClearFocus = () => {
    setIsFocusLocked(false);
    setLockedFocusNode(null);
    onSelectNode(null);
  };

  const handleLockCurrent = () => {
    if (selectedNodeRef.current) {
      setIsFocusLocked(true);
      setLockedFocusNode(selectedNodeRef.current);
    }
  };

  useEffect(() => {
    if (!containerRef.current) return;

    if (sigmaRef.current) {
      sigmaRef.current.kill();
      sigmaRef.current = null;
      graphRef.current = null;
    }

    const sigmaGraph = new Graph({ multi: true });
    nodes.forEach((node) => {
      sigmaGraph.addNode(node.id, {
        x: node.position.x,
        y: node.position.y,
        size: Math.max(0.7, node.data.size * 0.18),
        color: node.data.fill,
        baseSize: Math.max(0.7, node.data.size * 0.18),
        baseColor: node.data.fill,
        label: '',
      });
    });

    allEdges.forEach((edge) => {
      if (!sigmaGraph.hasNode(edge.source) || !sigmaGraph.hasNode(edge.target)) {
        return;
      }
      sigmaGraph.addEdgeWithKey(edge.id, edge.source, edge.target, {
        size: edge.isNested ? 0.55 : 0.8,
        color: edge.isNested ? '#f0b9c9' : '#b8d6ff',
        baseSize: edge.isNested ? 0.55 : 0.8,
        baseColor: edge.isNested ? '#f0b9c9' : '#b8d6ff',
      });
    });

    const renderer = new Sigma(sigmaGraph, containerRef.current, {
      renderLabels: false,
      labelDensity: 0.05,
      labelGridCellSize: 80,
      hideEdgesOnMove: false,
      allowInvalidContainer: true,
      minCameraRatio: 0.01,
      maxCameraRatio: 12,
      defaultNodeColor: '#94a3b8',
      defaultEdgeColor: '#93c5fd',
      zIndex: true,
    });

    const handleClickNode = ({ node }) => {
      onSelectNode(node);
    };
    const handleClickStage = () => {
      if (isFocusLockedRef.current) {
        return;
      }
      onSelectNode(null);
    };
    const camera = renderer.getCamera();
    const handleCamera = () => {
      const ratio = camera.getState().ratio || 1;
      setZoomLevel(1 / ratio);
    };

    renderer.on('clickNode', handleClickNode);
    renderer.on('clickStage', handleClickStage);
    camera.on('updated', handleCamera);
    handleCamera();

    sigmaRef.current = renderer;
    graphRef.current = sigmaGraph;

    return () => {
      camera.off('updated', handleCamera);
      renderer.off('clickNode', handleClickNode);
      renderer.off('clickStage', handleClickStage);
      renderer.kill();
      sigmaRef.current = null;
      graphRef.current = null;
    };
  }, [nodes, allEdges, onSelectNode]);

  useEffect(() => {
    const renderer = sigmaRef.current;
    const sigmaGraph = graphRef.current;
    if (!renderer || !sigmaGraph) return;

    const linkFocusNode = isFocusLocked && lockedFocusNode ? lockedFocusNode : selectedNode;

    const selectedNeighbors = new Set(adjacency.neighbors.get(linkFocusNode) || []);
    const selectedEdges = new Set(adjacency.incidentEdges.get(linkFocusNode) || []);

    sigmaGraph.forEachNode((node, attrs) => {
      if (!selectedNode) {
        sigmaGraph.mergeNodeAttributes(node, {
          color: attrs.baseColor || attrs.color,
          size: attrs.baseSize || attrs.size,
          hidden: false,
          zIndex: 0,
        });
        return;
      }

      const isSelected = node === selectedNode;
      const isLinkFocusNode = node === linkFocusNode;
      const isNeighbor = selectedNeighbors.has(node);
      if (isSelected) {
        sigmaGraph.mergeNodeAttributes(node, {
          color: '#111827',
          size: Math.max(2.8, (attrs.baseSize || attrs.size) * 3.2),
          hidden: false,
          zIndex: 20,
        });
      } else if (isLinkFocusNode) {
        sigmaGraph.mergeNodeAttributes(node, {
          color: '#1e3a8a',
          size: Math.max(2.0, (attrs.baseSize || attrs.size) * 2.2),
          hidden: false,
          zIndex: 18,
        });
      } else if (isNeighbor) {
        sigmaGraph.mergeNodeAttributes(node, {
          color: '#1d4ed8',
          size: Math.max(1.4, (attrs.baseSize || attrs.size) * 1.8),
          hidden: false,
          zIndex: 15,
        });
      } else {
        sigmaGraph.mergeNodeAttributes(node, {
          color: '#e5e7eb',
          size: Math.max(0.35, (attrs.baseSize || attrs.size) * 0.5),
          hidden: false,
          zIndex: 0,
        });
      }
    });

    sigmaGraph.forEachEdge((edge, attrs) => {
      if (!selectedNode) {
        sigmaGraph.mergeEdgeAttributes(edge, {
          color: attrs.baseColor || attrs.color,
          size: attrs.baseSize || attrs.size,
          hidden: false,
          zIndex: 0,
        });
        return;
      }

      const isSelectedEdge = selectedEdges.has(edge);
      if (isSelectedEdge) {
        sigmaGraph.mergeEdgeAttributes(edge, {
          color: '#1d4ed8',
          size: Math.max(2.6, (attrs.baseSize || attrs.size) * 3.3),
          hidden: false,
          zIndex: 25,
        });
      } else {
        sigmaGraph.mergeEdgeAttributes(edge, {
          color: '#f1f5f9',
          size: Math.max(0.1, (attrs.baseSize || attrs.size) * 0.2),
          hidden: true,
          zIndex: 0,
        });
      }
    });

    renderer.refresh();
  }, [selectedNode, adjacency, isFocusLocked, lockedFocusNode]);

  const renderedEdges = allEdges.length;

  return (
    <section className="granular-shell">
      <div className="graph-canvas-wrap">
        <div className="graph-overlay">
          <span>Granular overlay</span>
          <span>{nodes.length} points</span>
          <span>{renderedEdges} links shown</span>
          <span>{edgeStats.nested} nested links</span>
          <span>Zoom {zoomLevel.toFixed(2)}x</span>
          {isFocusLocked && lockedFocusNode ? <span>Links locked: {lockedFocusNode}</span> : null}
          {selectedNode ? (
            <>
              <button
                className="overlay-btn"
                type="button"
                onClick={isFocusLocked ? handleUnlockFocus : handleLockCurrent}
              >
                {isFocusLocked ? 'Unlock Focus' : 'Lock Focus'}
              </button>
              <button
                className="overlay-btn ghost"
                type="button"
                onClick={handleClearFocus}
              >
                Clear
              </button>
            </>
          ) : null}
        </div>
        <div ref={containerRef} className="sigma-canvas" />
      </div>

      <aside className="graph-sidepanel">
        <div className="side-card">
          <h3>Data Inspector</h3>
          {!selectedDetails && <p className="inspector-empty">Select any point to inspect full row data and linkage context.</p>}
          {selectedDetails && (
            <>
              <p><strong>Table:</strong> {selectedDetails.table}</p>
              <p><strong>Row:</strong> {selectedDetails.rowId}</p>
              <p><strong>Connections:</strong> {selectedDetails.degree}</p>
              <FieldList fields={selectedDetails.fields} />
            </>
          )}
        </div>

        <div className="side-card chat-mock">
          <h3>Chat with Graph</h3>
          <p className="muted">Order to Cash</p>
          <p>Ask questions about a selected row or linked records.</p>
          <div className="chat-input-row">
            <input disabled placeholder="Analyze anything" />
            <button disabled>Send</button>
          </div>
        </div>
      </aside>
    </section>
  );
}
