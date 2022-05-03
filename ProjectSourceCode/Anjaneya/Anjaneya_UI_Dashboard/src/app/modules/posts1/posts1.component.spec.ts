import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Posts1Component } from './posts1.component';

describe('Posts1Component', () => {
  let component: Posts1Component;
  let fixture: ComponentFixture<Posts1Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Posts1Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Posts1Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
