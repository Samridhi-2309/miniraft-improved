function createCanvas(canvasEl) {
  const ctx = canvasEl.getContext('2d');
  let drawing = false;
  let points = [];
  let color = '#000000';
  let strokeCallback = () => {};
  let interactive = true;
  const strokeHistory = [];

  // Hi-DPI support
  function resizeForHiDPI() {
    const dpr = window.devicePixelRatio || 1;
    const rect = canvasEl.getBoundingClientRect();
    canvasEl.width = rect.width * dpr;
    canvasEl.height = rect.height * dpr;
    canvasEl.style.width = rect.width + 'px';
    canvasEl.style.height = rect.height + 'px';
    ctx.scale(dpr, dpr);
    ctx.lineCap = 'round';
    ctx.lineJoin = 'round';
    ctx.lineWidth = 2.5;
  }

  function setColor(c) { 
    color = c;
    ctx.globalCompositeOperation = (c === '#FFFFFF' || c === '#FFF') ? 'destination-out' : 'source-over';
  }

  function clear() {
    ctx.clearRect(0, 0, canvasEl.width, canvasEl.height);
    strokeHistory.length = 0;
  }

  function redraw() {
    ctx.clearRect(0, 0, canvasEl.width, canvasEl.height);
    ctx.globalCompositeOperation = 'source-over';
    strokeHistory.forEach(stroke => {
      drawStrokeImmediate(stroke.points, stroke.color);
    });
  }

  function drawLineSegment(a, b, strokeColor) {
    const isEraser = strokeColor === '#FFFFFF' || strokeColor === '#FFF';
    if (isEraser) {
      ctx.globalCompositeOperation = 'destination-out';
      ctx.strokeStyle = 'rgba(0,0,0,1)';
    } else {
      ctx.globalCompositeOperation = 'source-over';
      ctx.strokeStyle = strokeColor || color;
    }
    ctx.lineWidth = isEraser ? 15 : 2.5;
    ctx.beginPath();
    ctx.moveTo(a.x, a.y);
    ctx.lineTo(b.x, b.y);
    ctx.stroke();
  }

  function drawStrokeImmediate(pts, strokeColor) {
    for (let i = 1; i < pts.length; i++) {
      drawLineSegment(pts[i - 1], pts[i], strokeColor);
    }
    ctx.globalCompositeOperation = 'source-over';
  }

  function drawRemoteStroke(stroke) {
    if (!stroke || !Array.isArray(stroke.points)) return;
    strokeHistory.push({
      type: 'stroke',
      points: stroke.points.slice(),
      color: stroke.color || '#000',
      timestamp: stroke.timestamp || Date.now()
    });
    drawStrokeImmediate(stroke.points, stroke.color || '#000');
  }

  function pointerPos(evt) {
    const rect = canvasEl.getBoundingClientRect();
    return { x: evt.clientX - rect.left, y: evt.clientY - rect.top };
  }

  function startPoint(pt) {
    if (!interactive) return;
    drawing = true;
    points = [pt];
  }

  function movePoint(pt) {
    if (!drawing || !interactive) return;
    points.push(pt);
    const len = points.length;
    if (len > 1) drawLineSegment(points[len - 2], points[len - 1], color);
  }

  function endStroke() {
    if (!drawing || !interactive) return;
    drawing = false;
    if (points.length > 0) {
      const stroke = {
        type: 'stroke',
        commandId: crypto.randomUUID(),
        points: points.slice(),
        color,
        timestamp: Date.now()
      };
      strokeHistory.push(stroke);
      strokeCallback(stroke);
    }
  }

  function setInteractive(enabled) {
    interactive = Boolean(enabled);
    canvasEl.style.pointerEvents = interactive ? 'auto' : 'none';
  }

  function undo() {
    if (strokeHistory.length > 0) {
      strokeHistory.pop();
      redraw();
    }
  }

  // Events
  canvasEl.addEventListener('pointerdown', (e) => {
    canvasEl.setPointerCapture(e.pointerId);
    startPoint(pointerPos(e));
  });

  canvasEl.addEventListener('pointermove', (e) => {
    movePoint(pointerPos(e));
  });

  canvasEl.addEventListener('pointerup', (e) => {
    canvasEl.releasePointerCapture(e.pointerId);
    endStroke();
  });

  canvasEl.addEventListener('pointercancel', () => { endStroke(); });

  window.addEventListener('resize', () => { resizeForHiDPI(); });
  // initial resize for current canvas size
  setTimeout(resizeForHiDPI, 0);

  return {
    setColor,
    clear,
    drawRemoteStroke,
    undo,
    setInteractive,
    onStrokeComplete(cb) { strokeCallback = cb; }
  };
}

// Export for simple import pattern when included via <script>
window.createCanvas = createCanvas;
