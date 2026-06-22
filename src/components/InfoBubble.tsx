import * as React from 'react';

export interface IInfoBubbleProps {
  text: string;
  id?: string;
}

/**
 * A small accessible info bubble: an `ⓘ` glyph that reveals help text in a
 * tooltip on hover or keyboard focus (PRD §6.1.3). The tooltip is linked via
 * `aria-describedby`, opens on hover/focus/click, and dismisses on Escape or
 * blur — so it is reachable by mouse, keyboard, and touch. Used by the form
 * DescriptionFieldTemplate so every DAG and NODE field gets one.
 */
export function InfoBubble(props: IInfoBubbleProps): JSX.Element {
  const [open, setOpen] = React.useState(false);
  const tipId = (props.id ?? 'field') + '__tip';
  return (
    <span
      className="jp-afdag-info-bubble"
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
    >
      <button
        type="button"
        className="jp-afdag-info-glyph"
        aria-label="More information"
        aria-describedby={open ? tipId : undefined}
        aria-expanded={open}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
        onKeyDown={event => {
          if (event.key === 'Escape') {
            setOpen(false);
          }
        }}
        onClick={event => {
          // Touch has no hover; toggle on tap. Don't bubble into the form/canvas.
          event.preventDefault();
          event.stopPropagation();
          setOpen(o => !o);
        }}
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
