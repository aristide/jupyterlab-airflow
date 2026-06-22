import * as React from 'react';

export interface IInfoBubbleProps {
  text: string;
  id?: string;
}

/**
 * A small accessible info bubble: an `ⓘ` glyph that reveals help text in a
 * tooltip on hover or keyboard focus (PRD §6.1.3). The tooltip is linked via
 * `aria-describedby`; it opens on hover or focus — tapping the glyph focuses it,
 * which covers touch — and dismisses on Escape, blur, or mouse-leave. Used by
 * the form DescriptionFieldTemplate so every DAG and NODE field gets one.
 */
export function InfoBubble(props: IInfoBubbleProps): JSX.Element {
  // Track hover and focus independently so releasing one source doesn't close
  // the tooltip while the other is still active (a stray mouse-leave must not
  // hide a keyboard-focused bubble, and vice-versa). Escape hides it until the
  // pointer/focus leaves and returns. No click toggle — it would race onFocus
  // (a click focuses first) and close a just-opened bubble.
  const [hovered, setHovered] = React.useState(false);
  const [focused, setFocused] = React.useState(false);
  const [dismissed, setDismissed] = React.useState(false);
  const open = (hovered || focused) && !dismissed;
  const tipId = (props.id ?? 'field') + '__tip';
  return (
    <span
      className="jp-afdag-info-bubble"
      onMouseEnter={() => {
        setHovered(true);
        setDismissed(false);
      }}
      onMouseLeave={() => setHovered(false)}
    >
      <button
        type="button"
        className="jp-afdag-info-glyph"
        aria-label="More information"
        aria-describedby={tipId}
        onFocus={() => {
          setFocused(true);
          setDismissed(false);
        }}
        onBlur={() => setFocused(false)}
        onKeyDown={event => {
          if (event.key === 'Escape') {
            setDismissed(true);
          }
        }}
        onClick={event => event.stopPropagation()}
      >
        ⓘ
      </button>
      <span
        id={tipId}
        role="tooltip"
        className="jp-afdag-tooltip"
        hidden={!open}
      >
        {props.text}
      </span>
    </span>
  );
}
