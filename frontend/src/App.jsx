import { useState } from 'react'
import MainMenu from './components/MainMenu.jsx'
import AppenderTool from './components/appender/AppenderTool.jsx'

/**
 * App is the root component. It owns the current view state and renders
 * the shell (header + content area). Adding a new tool means:
 *  1. Adding a new view key here (e.g. 'vocabulary')
 *  2. Creating a new component under components/<feature>/
 *  3. Adding a card to MainMenu and a case in renderView()
 */
export default function App() {
  const [view, setView] = useState('menu')      // 'menu' | 'appender' | ...
  const [subtitle, setSubtitle] = useState('Select a utility')
  const [dryRun, setDryRun] = useState(true)    // lifted so header badge can read it

  function openTool(toolId, label) {
    setView(toolId)
    setSubtitle(label)
  }

  function goMenu() {
    setView('menu')
    setSubtitle('Select a utility')
  }

  function renderView() {
    switch (view) {
      case 'appender':
        return <AppenderTool dryRun={dryRun} setDryRun={setDryRun} />
      default:
        return <MainMenu onOpen={openTool} />
    }
  }

  return (
    <div className="shell">
      <header>
        <div className="header-left">
          {view !== 'menu' && (
            <button className="btn sm" onClick={goMenu}>← Menu</button>
          )}
          <span className="logo">
            OMOP<span className="logo-sep">/</span>Suite
          </span>
        </div>
        <span className="logo-sub">{subtitle}</span>
        <span className="header-spacer" />
        {view === 'appender' && (
          <span className={`run-badge show${!dryRun ? ' live' : ''}`}>
            {dryRun ? 'dry-run' : 'live'}
          </span>
        )}
      </header>

      {renderView()}
    </div>
  )
}
