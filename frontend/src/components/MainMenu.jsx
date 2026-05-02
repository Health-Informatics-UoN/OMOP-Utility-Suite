/**
 * Main menu — the landing page showing all available tools.
 * To add a new tool, add an entry to TOOLS and create its component.
 */

const TOOLS = [
  {
    id:       'appender',
    icon:     '⬡',
    label:    'OMOP Appender',
    desc:     'Sync and deduplicate patient records and clinical events from a source to a target OMOP database.',
    subtitle: 'auto-diff patient appender · PostgreSQL',
    disabled: false,
  },
  {
    id:       'vocabulary',
    icon:     '▤',
    label:    'Vocabulary Updater',
    desc:     'Coming soon. Automate the ingestion and mapping of new Athena vocabulary releases.',
    disabled: true,
  },
  {
    id:       'nlp-etl',
    icon:     '◱',
    label:    'OMOP NLP ETL',
    desc:     'Coming soon. Translate natural language processing results into OMOP data.',
    disabled: true,
  },
]

export default function MainMenu({ onOpen }) {
  return (
    <div id="main-menu-view" className="view-panel">
      <div className="ptitle" style={{ marginTop: '2rem' }}>Welcome</div>
      <div className="phead">OMOP Database Utility Suite</div>
      <p className="hint bottom-gap">Select a module below to begin your database workflow.</p>

      <div className="utility-grid">
        {TOOLS.map(tool => (
          <div
            key={tool.id}
            className={`card utility-card${tool.disabled ? ' disabled' : ''}`}
            onClick={() => !tool.disabled && onOpen(tool.id, tool.subtitle)}
          >
            <div className="utility-icon">{tool.icon}</div>
            <div className="ctitle">{tool.label}</div>
            <div className="cdesc">{tool.desc}</div>
          </div>
        ))}
      </div>
    </div>
  )
}
